import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any, Callable, Dict, Generic, List, Literal, Optional, Tuple, Type, TypeVar, cast

import boto3
import pendulum
import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")

BASE_ORB_API_URL = "https://api.withorb.com/v1/"
PAGE_SIZE = 20
MAX_PAGES_PER_SYNC = 10

## Only applies to costs
MIN_COSTS_START_TIME = pendulum.datetime(2023, 1, 1, 0, 0, 0, tz="UTC")
COSTS_TIMEFRAME_WINDOW = 10
GRACE_PERIOD_BUFFER_DAYS = 2

## Ledger Entries
LEDGER_ENTRIES_PAGE_SIZE = 300

## Customer Resource Events
CUSTOMER_RESOURCE_EVENTS_PAGE_SIZE = 200

CURRENT_STATE_VERSION = 4
REQUEST_TIMEOUT = 10
req_session = requests.Session()
retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
req_session.mount("https://", HTTPAdapter(max_retries=retries))
req_session.headers.update({"User-Agent": "Orb-Fivetran-Connector"})


class AbstractCursor(ABC):
    @abstractmethod
    def serialize(self) -> Dict[str, Any]:
        ...

    @staticmethod
    @abstractmethod
    def deserialize(d: Dict[str, Any]) -> "AbstractCursor":
        ...

    @staticmethod
    @abstractmethod
    def min() -> "AbstractCursor":
        ...

    @abstractmethod
    def pagination_exhausted(self) -> bool:
        ...


@dataclass(frozen=True)
class SimplePaginationCursor(AbstractCursor):
    """
    This is a simple cursor that just keeps track of a pagination cursor string
    but does not keep track of a minimum time like `Cursor` does. This is only used
    when paginating slices for a nested resource.
    """

    has_more: bool = True
    current_cursor: Optional[str] = None

    def serialize(self) -> Dict[str, Any]:
        return {"has_more": self.has_more, "current_cursor": self.current_cursor}

    @staticmethod
    def deserialize(d: Dict[str, Any]) -> "SimplePaginationCursor":
        return SimplePaginationCursor(has_more=d["has_more"], current_cursor=d["current_cursor"])

    @staticmethod
    def min() -> "SimplePaginationCursor":
        return SimplePaginationCursor(has_more=True, current_cursor=None)

    def pagination_exhausted(self) -> bool:
        return self.has_more is False


@dataclass(frozen=True)
class Cursor(AbstractCursor):
    """
    When we paginate, we get results that are greater than
    or equal to `min_time_cursor`. Since results are ordered
    *newest* to *oldest*, the first page returned gives us the time
    boundary of how to advance after pagination is exhausted
    and we set this as `min_time_after_pagination_exhausted`, but
    until we're still paging through this result set, we use
    `min_time_cursor`.
    """

    # Relate to "current" result set
    min_time_cursor: pendulum.DateTime
    maybe_pagination_cursor: Optional[str]

    # This will become `min_time_cursor` after
    # pagination is exhausted
    min_time_after_pagination_exhausted: Optional[pendulum.DateTime] = None

    def pagination_exhausted(self) -> bool:
        return self.maybe_pagination_cursor is None and self.min_time_after_pagination_exhausted is None

    @staticmethod
    def min() -> "Cursor":
        return Cursor(
            min_time_cursor=pendulum.from_timestamp(0), maybe_pagination_cursor=None, min_time_after_pagination_exhausted=None
        )

    def with_updated_pagination_result(
        self, pagination_cursor: Optional[str], max_time_in_page: Optional[pendulum.DateTime]
    ) -> "Cursor":
        # No more pages left, and pagination is exhausted
        if pagination_cursor is None:
            return Cursor(
                # We will set to `max_time_in_page` in the case that `min_time_after_pagination_exhausted` is None,
                # which means that we only had a single page of results. If max_time_in_page is None, that means
                # that there were no results so we want to maintain the `min_time_cursor`
                min_time_cursor=self.min_time_after_pagination_exhausted or max_time_in_page or self.min_time_cursor,
                maybe_pagination_cursor=None,
                min_time_after_pagination_exhausted=None,
            )
        # We are paginating
        elif pagination_cursor is not None:
            # If we are paginating, there should be something in the current page,
            # so max_time_in_page should be non-null
            assert max_time_in_page is not None
            return Cursor(
                min_time_cursor=self.min_time_cursor,
                maybe_pagination_cursor=pagination_cursor,
                # If we don't already have a `min_time_after_pagination_exhausted`, then set this
                # to the max time in this page, because that means this is the first page result
                # Note that this assumes we're paginating *newest* to *oldest*
                min_time_after_pagination_exhausted=self.min_time_after_pagination_exhausted or max_time_in_page,
            )

    def serialize(self):
        return {
            "min_time_cursor": self.min_time_cursor.isoformat(),
            "maybe_pagination_cursor": self.maybe_pagination_cursor,
            "min_time_after_pagination_exhausted": self.min_time_after_pagination_exhausted.isoformat()
            if self.min_time_after_pagination_exhausted
            else None,
        }

    @staticmethod
    def deserialize(serialized_state) -> "Cursor":
        def maybe_parse_as_datetime(datetime_field) -> Optional[pendulum.DateTime]:
            return cast(pendulum.DateTime, pendulum.parse(datetime_field)) if datetime_field is not None else None

        return Cursor(
            min_time_cursor=pendulum.parse(serialized_state["min_time_cursor"]),  # type: ignore
            maybe_pagination_cursor=serialized_state["maybe_pagination_cursor"],
            min_time_after_pagination_exhausted=maybe_parse_as_datetime(serialized_state["min_time_after_pagination_exhausted"]),
        )


@dataclass
class CursorWithSlices(AbstractCursor):
    """
    A wrapper around Cursor that allows us to store a cursor for each slice.
    Used for Nested Streams.
    """

    per_slice_cursor: Dict[str, Cursor]

    # This is so that we can avoid fetching all slices in every invocation,
    # so it's a cursor for the slice list itself (not a cursor for a slice)
    # `CursorWithSlices` is fully exhausted when `slices_pagination_cursor` is None
    # and each cursor in `per_slice_cursor` is exhausted
    slices_pagination_cursor: SimplePaginationCursor

    def cursor_for_slice(self, slice_id):
        return self.per_slice_cursor.get(slice_id, Cursor.min())

    def update_cursor_for_slice(self, slice_id, cursor):
        self.per_slice_cursor[slice_id] = cursor

    def update_slices_pagination_cursor(self, cursor):
        self.slices_pagination_cursor = cursor

    def all_current_slices_exhausted(self):
        return all(cursor.pagination_exhausted() for cursor in self.per_slice_cursor.values())

    def pagination_exhausted(self) -> bool:
        return self.slices_pagination_cursor.pagination_exhausted() and all(
            self.cursor_for_slice(slice_id).pagination_exhausted() for slice_id in self.per_slice_cursor.keys()
        )

    def serialize(self):
        return {
            "per_slice_cursor": {slice_id: cursor.serialize() for slice_id, cursor in self.per_slice_cursor.items()},
            "slices_pagination_cursor": self.slices_pagination_cursor.serialize(),
        }

    @staticmethod
    def min() -> "CursorWithSlices":
        return CursorWithSlices(per_slice_cursor={}, slices_pagination_cursor=SimplePaginationCursor.min())

    @staticmethod
    def deserialize(serialized_state) -> "CursorWithSlices":
        return CursorWithSlices(
            per_slice_cursor={
                slice_id: Cursor.deserialize(cursor) for slice_id, cursor in serialized_state["per_slice_cursor"].items()
            },
            slices_pagination_cursor=SimplePaginationCursor.deserialize(serialized_state["slices_pagination_cursor"]),
        )


@dataclass
class SubscriptionCostsCursor(AbstractCursor):
    """
    A cursor for the SubscriptionCosts stream,
    where we get costs for each timeframe across
    all subscriptions.

    The pagination strategy is to go through all subscriptions
    for a given timeframe before advancing the timeframe. If we're
    in the middle of paginating subscriptions, then
    `current_subscriptions_pagination_cursor` will be non-None.
    """

    current_timeframe_start: pendulum.DateTime
    # The reason we store both start and end is so that we can
    # keep the timeframe constant while we're still paginating subscriptions
    # for this specific timeframe. We don't want later runs to have larger
    # timeframes.
    current_timeframe_end: pendulum.DateTime
    current_subscriptions_pagination_cursor: Optional[str]

    def serialize(self):
        return {
            "current_timeframe_start": self.current_timeframe_start.isoformat(),
            "current_timeframe_end": self.current_timeframe_end.isoformat(),
            "current_subscriptions_pagination_cursor": self.current_subscriptions_pagination_cursor,
        }

    @staticmethod
    def deserialize(serialized_state) -> "SubscriptionCostsCursor":
        current_timeframe_start_str = serialized_state["current_timeframe_start"]
        current_timeframe_end_str = serialized_state["current_timeframe_end"]
        return SubscriptionCostsCursor(
            current_timeframe_start=pendulum.parse(current_timeframe_start_str, tz="UTC"),
            current_timeframe_end=pendulum.parse(current_timeframe_end_str, tz="UTC"),
            current_subscriptions_pagination_cursor=serialized_state["current_subscriptions_pagination_cursor"],
        )

    @staticmethod
    def calculate_end_time_from_start_time(start_time: pendulum.DateTime) -> pendulum.DateTime:
        return min(
            start_time.add(days=COSTS_TIMEFRAME_WINDOW),
            pendulum.now("UTC").subtract(days=GRACE_PERIOD_BUFFER_DAYS),
        )

    @staticmethod
    def min() -> "SubscriptionCostsCursor":
        return SubscriptionCostsCursor(
            current_timeframe_start=MIN_COSTS_START_TIME,
            current_timeframe_end=SubscriptionCostsCursor.calculate_end_time_from_start_time(MIN_COSTS_START_TIME),
            current_subscriptions_pagination_cursor=None,
        )

    @staticmethod
    def maybe_deserialize_state(state) -> "SubscriptionCostsCursor":
        if state.get("subscription_costs"):
            return SubscriptionCostsCursor.deserialize(state["subscription_costs"])
        else:
            return SubscriptionCostsCursor.min()

    def advance_timeframe_bounds(self, timeframe_end):
        assert self.current_subscriptions_pagination_cursor is None
        self.current_timeframe_start = timeframe_end
        self.current_timeframe_end = SubscriptionCostsCursor.calculate_end_time_from_start_time(self.current_timeframe_start)

    def update_subscription_pagination_for_current_timeframe(self, pagination_cursor: Optional[str]):
        """
        This advances which subscription we're on, while staying on the same timeframe.
        """
        self.current_subscriptions_pagination_cursor = pagination_cursor

    def mark_current_timeframe_exhausted(self):
        self.current_subscriptions_pagination_cursor = None

    def pagination_exhausted(self) -> bool:
        """
        Pagination is exhausted if we have < 1 day in our timeframe.
        """
        is_exhausted = (
            self.current_subscriptions_pagination_cursor is None
            and (self.current_timeframe_end - self.current_timeframe_start).days < 1
        )
        return is_exhausted


T = TypeVar("T", bound=AbstractCursor)


class AbstractResourceFetchResponse(ABC, Generic[T]):
    @abstractmethod
    def all_resources(self):
        pass

    @abstractmethod
    def get_updated_state(self) -> T:
        pass

    @staticmethod
    @abstractmethod
    def init_with_cursor(cursor: T, resource_config) -> "AbstractResourceFetchResponse[T]":
        pass


class AbstractResourceFetcher(ABC, Generic[T]):
    def __init__(self, authorization_header, resource_config):
        self.auth_header = authorization_header
        self.resource_config = resource_config

    @abstractmethod
    def fetch_after_cursor(self, initial_cursor: T) -> AbstractResourceFetchResponse[T]:
        pass

    def fetch_all(self, maybe_existing_state_cursor: T) -> AbstractResourceFetchResponse[T]:
        initial_cursor = (
            maybe_existing_state_cursor if maybe_existing_state_cursor is not None else type(maybe_existing_state_cursor).min()
        )
        return self.fetch_after_cursor(initial_cursor)

    def fetch_resources_from_path(
        self, path, pagination_cursor: Optional[str] = None, max_pages=MAX_PAGES_PER_SYNC
    ) -> Tuple[List[Any], SimplePaginationCursor]:
        """
        Exhaustively fetches resources from the passed in path. Note that
        this needs to be called on every invocation of this sync, so
        a very expensive parent resource will cause issues
        """
        params = {"limit": getattr(self.resource_config, "page_size", PAGE_SIZE)}
        if pagination_cursor:
            params["cursor"] = pagination_cursor

        response_json = req_session.get(
            BASE_ORB_API_URL + path,
            headers=self.auth_header,
            params=params,
        ).json()
        resources = response_json["data"]
        num_pages = 1
        has_more = response_json["pagination_metadata"]["has_more"]
        while has_more and num_pages < max_pages:
            params["cursor"] = response_json["pagination_metadata"]["next_cursor"]
            response_json = req_session.get(
                BASE_ORB_API_URL + path,
                headers=self.auth_header,
                params=params,
            ).json()
            resources.extend(response_json["data"])
            num_pages += 1
            has_more = response_json["pagination_metadata"]["has_more"]

        return (
            resources,
            SimplePaginationCursor(has_more=has_more, current_cursor=response_json["pagination_metadata"]["next_cursor"]),
        )


@dataclass(frozen=True)
class BaseResourceConfig:
    primary_key_list: List[str]
    resultant_schema_key: str
    maybe_fetcher_type: Optional[Type[AbstractResourceFetcher]]


@dataclass(frozen=True)
class ResourceConfig(BaseResourceConfig):
    api_path: str
    # This determines what API field we use
    # to store state in between syncs.
    maybe_state_time_attribute: Optional[str]
    fetch_strategy: Literal["direct_fetch", "resource_events"]
    page_size: int = PAGE_SIZE


@dataclass(frozen=True)
class NestedResourceConfig(BaseResourceConfig):
    """
    Config for a nested resource, which is a resource that is nested under a parent resource
    and cannot be paginated directly.
    """

    # The path to a list of parent resources
    all_slices_api_path: str
    # The path to a single slice
    single_slice_api_path: Callable[[str], str]
    maybe_state_time_attribute: Optional[str]
    page_size: int = PAGE_SIZE

    def slice_resource_config(self, slice_id: str) -> ResourceConfig:
        """
        Returns a ResourceConfig for a single slice of this nested resource,
        given the slice_id.
        """
        return ResourceConfig(
            api_path=self.single_slice_api_path(slice_id),  # type:ignore
            maybe_state_time_attribute=self.maybe_state_time_attribute,
            resultant_schema_key=self.resultant_schema_key,
            # Only support direct fetch for now for nested resources
            fetch_strategy="direct_fetch",
            primary_key_list=self.primary_key_list,
            page_size=self.page_size,
            maybe_fetcher_type=None,
        )


def fetcher_for_resource_config(resource_config) -> Type[AbstractResourceFetcher]:
    if resource_config.maybe_fetcher_type is not None:
        return resource_config.maybe_fetcher_type
    elif type(resource_config) == NestedResourceConfig:
        return NestedResourceFetcher
    elif resource_config.fetch_strategy == "direct_fetch":
        return DirectResourceFetcher
    else:
        return ResourceEventBasedResourceFetcher


@dataclass
class FunctionState:
    customer_cursor: AbstractCursor
    plan_cursor: AbstractCursor
    # This cursor only applies to issued invoices.
    # We always sync all draft invoices available.
    invoice_cursor: AbstractCursor
    draft_invoice_cursor: AbstractCursor
    subscription_cursor: AbstractCursor
    subscription_version_cursor: AbstractCursor
    subscription_costs_cursor: AbstractCursor
    credit_ledger_entry_cursor: AbstractCursor

    def as_dict(self):
        return {
            "state_version": CURRENT_STATE_VERSION,
            "customer": self.customer_cursor.serialize(),
            "plan": self.plan_cursor.serialize(),
            "invoice": self.invoice_cursor.serialize(),
            "draft_invoice": self.draft_invoice_cursor.serialize(),
            "subscription": self.subscription_cursor.serialize(),
            "subscription_version": self.subscription_version_cursor.serialize(),
            "credit_ledger_entry": self.credit_ledger_entry_cursor.serialize(),
            "subscription_costs": self.subscription_costs_cursor.serialize(),
        }

    @staticmethod
    def from_serialized_state(state: Optional[Dict]):
        if state is None or state.get("state_version") != CURRENT_STATE_VERSION:
            logger.info("Discarding current state entirely.")
            return FunctionState(
                customer_cursor=Cursor.min(),
                plan_cursor=Cursor.min(),
                invoice_cursor=Cursor.min(),
                draft_invoice_cursor=Cursor.min(),
                subscription_cursor=Cursor.min(),
                subscription_version_cursor=Cursor.min(),
                credit_ledger_entry_cursor=CursorWithSlices.min(),
                subscription_costs_cursor=SubscriptionCostsCursor.min(),
            )
        else:

            def maybe_deserialize_resource_state(state: Dict, resource_name):
                if state.get(resource_name) is not None:
                    return Cursor.deserialize(serialized_state=state[resource_name])
                else:
                    return Cursor.min()

            def maybe_deserialize_nested_resource_state(state: Dict, resource_name):
                if state.get(resource_name) is not None:
                    return CursorWithSlices.deserialize(serialized_state=state[resource_name])
                else:
                    return CursorWithSlices.min()

            return FunctionState(
                customer_cursor=maybe_deserialize_resource_state(state, "customer"),
                plan_cursor=maybe_deserialize_resource_state(state, "plan"),
                invoice_cursor=maybe_deserialize_resource_state(state, "invoice"),
                subscription_cursor=maybe_deserialize_resource_state(state, "subscription"),
                draft_invoice_cursor=maybe_deserialize_resource_state(state, "draft_invoice"),
                subscription_version_cursor=maybe_deserialize_resource_state(state, "subscription_version"),
                credit_ledger_entry_cursor=maybe_deserialize_nested_resource_state(state, "credit_ledger_entry"),
                subscription_costs_cursor=SubscriptionCostsCursor.maybe_deserialize_state(state),
            )

    def state_cursor_for_resource(self, resource) -> AbstractCursor:
        assert resource in RESOURCE_TO_RESOURCE_CONFIG.keys()
        if resource == "customer":
            return self.customer_cursor
        elif resource == "plan":
            return self.plan_cursor
        elif resource == "subscription":
            return self.subscription_cursor
        elif resource == "issued_invoice":
            return self.invoice_cursor
        elif resource == "draft_invoice":
            return self.draft_invoice_cursor
        elif resource == "subscription_version":
            return self.subscription_version_cursor
        elif resource == "credit_ledger_entry":
            return self.credit_ledger_entry_cursor
        elif resource == "subscription_costs":
            return self.subscription_costs_cursor
        else:
            raise Exception(f"Cannot get state for resource {resource}")

    def set_for_resource(self, resource: str, cursor: AbstractCursor) -> None:
        assert resource in RESOURCE_TO_RESOURCE_CONFIG.keys()
        if resource == "subscription":
            self.subscription_cursor = cursor
        elif resource == "customer":
            self.customer_cursor = cursor
        elif resource == "plan":
            self.plan_cursor = cursor
        elif resource == "issued_invoice":
            self.invoice_cursor = cursor
        elif resource == "draft_invoice":
            self.draft_invoice_cursor = cursor
        elif resource == "subscription_version":
            self.subscription_version_cursor = cursor
        elif resource == "credit_ledger_entry":
            self.credit_ledger_entry_cursor = cursor
        elif resource == "subscription_costs":
            self.subscription_costs_cursor = cursor
        else:
            raise Exception(f"Cannot set state for resource {resource}")


@dataclass
class FivetranFunctionResponse:
    state: FunctionState
    insert: Dict = field(default_factory=dict)
    schema: Dict = field(
        default_factory=lambda: {
            "customer": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["customer"].primary_key_list},
            "plan": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["plan"].primary_key_list},
            "invoice": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["issued_invoice"].primary_key_list},
            "subscription": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["subscription"].primary_key_list},
            "credit_ledger_entry": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["credit_ledger_entry"].primary_key_list},
            "subscription_costs": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["subscription_costs"].primary_key_list},
            # A subscription version is uniquely identified by a subscription ID and a
            # start date given that the start date is in the past (which is the only resources
            # we'll sync here.)
            "subscription_version": {"primary_key": RESOURCE_TO_RESOURCE_CONFIG["subscription_version"].primary_key_list},
        }
    )
    has_more: bool = False

    def as_dict(self, s3_sync=False):
        if s3_sync:
            return {
                "state": self.state.as_dict(),
                "schema": self.schema,
                "hasMore": self.has_more,
            }
        else:
            return {
                "state": self.state.as_dict(),
                "insert": self.insert,
                "delete": {},
                "schema": self.schema,
                "hasMore": self.has_more,
            }

    def serialize_output_to_s3(self):
        return {"insert": self.insert, "delete": {}}

    @staticmethod
    def test_response():
        return FivetranFunctionResponse(
            state=FunctionState.from_serialized_state(None), insert={}, schema={}, has_more=False
        ).as_dict()


@dataclass
class ResourceFetchResponse(AbstractResourceFetchResponse[Cursor]):
    resource_id_to_resource: Dict[str, Any]
    updated_state: Cursor
    resource_config: ResourceConfig

    def all_resources(self):
        return list(self.resource_id_to_resource.values())

    def add_resource(self, resource):
        # Note that this first-write-wins behavior assumes that we're paginating
        # *newest* to *oldest*
        primary_key = ""
        for element in self.resource_config.primary_key_list:
            primary_key += resource[element]

        if primary_key not in self.resource_id_to_resource.keys():
            self.resource_id_to_resource[primary_key] = resource

    @staticmethod
    def init_with_cursor(cursor: Cursor, resource_config: ResourceConfig) -> "ResourceFetchResponse":
        return ResourceFetchResponse(resource_id_to_resource={}, updated_state=cursor, resource_config=resource_config)

    def get_updated_state(self) -> Cursor:
        return self.updated_state


@dataclass
class NestedResourceFetchResponse(AbstractResourceFetchResponse[CursorWithSlices]):
    slice_id_to_resource_fetch_response: Dict[str, ResourceFetchResponse]
    updated_state: CursorWithSlices
    nested_resource_config: NestedResourceConfig

    @staticmethod
    def init_with_cursor(cursor: CursorWithSlices, resource_config: NestedResourceConfig) -> "NestedResourceFetchResponse":
        return NestedResourceFetchResponse(
            slice_id_to_resource_fetch_response={}, updated_state=cursor, nested_resource_config=resource_config
        )

    def update_all_slices_cursor(self, cursor: SimplePaginationCursor):
        self.updated_state.update_slices_pagination_cursor(cursor)

    def add_response_for_slice(self, slice_id: str, response: ResourceFetchResponse):
        self.slice_id_to_resource_fetch_response[slice_id] = response
        self.updated_state.update_cursor_for_slice(slice_id, response.updated_state)

    def all_resources(self):
        for response in self.slice_id_to_resource_fetch_response.values():
            yield from response.all_resources()

    def top_level_slices_pagination_cursor(self):
        return self.updated_state.slices_pagination_cursor

    def get_updated_state(self) -> CursorWithSlices:
        return self.updated_state


@dataclass
class PeriodicSubscriptionCost:
    subscription_id: str
    timeframe_start: str
    timeframe_end: str
    subtotal: Decimal
    total: Decimal
    price_id: str
    grouped_costs_json: str

    def serialize(self):
        return {
            "subscription_id": self.subscription_id,
            "timeframe_start": self.timeframe_start,
            "timeframe_end": self.timeframe_end,
            "subtotal": self.subtotal,
            "total": self.total,
            "price_id": self.price_id,
            "grouped_costs_json": self.grouped_costs_json,
        }


@dataclass
class SubscriptionCostsFetchResponse(AbstractResourceFetchResponse[SubscriptionCostsCursor]):
    subscription_costs: List[PeriodicSubscriptionCost]
    updated_state: SubscriptionCostsCursor
    resource_config: ResourceConfig

    def all_resources(self):
        return [cost.serialize() for cost in self.subscription_costs]

    def add_resource(self, resource):
        self.subscription_costs.append(resource)

    def update_cursor_state(self, subscriptions_cursor: SimplePaginationCursor, timeframe_end: pendulum.DateTime):
        if subscriptions_cursor.current_cursor is None:
            self.updated_state.mark_current_timeframe_exhausted()
            self.updated_state.advance_timeframe_bounds(timeframe_end=timeframe_end)
        else:
            self.updated_state.update_subscription_pagination_for_current_timeframe(subscriptions_cursor.current_cursor)

    @staticmethod
    def init_with_cursor(cursor: SubscriptionCostsCursor, resource_config: ResourceConfig) -> "SubscriptionCostsFetchResponse":
        return SubscriptionCostsFetchResponse(subscription_costs=[], updated_state=cursor, resource_config=resource_config)

    def get_updated_state(self) -> SubscriptionCostsCursor:
        return self.updated_state


class DirectResourceFetcher(AbstractResourceFetcher[Cursor]):
    """
    Used for resources that are not fetched via resource events
    and are queried directly, such as SubscriptionVersion(s).
    """

    def fetch_after_cursor(self, initial_cursor: Cursor) -> ResourceFetchResponse:
        supports_incremental_syncs = self.resource_config.maybe_state_time_attribute is not None
        """
        Fetches resources after the passed in cursor. Note that this will also attempt to paginate
        up to MAX_PAGES_PER_SYNC times.

        Since this is the `Direct` fetcher, it queries for resources directly based on a time attribute
        of the resource.
        """
        params: Dict[str, Any] = {"limit": self.resource_config.page_size}

        def update_with_new_result_page(response: ResourceFetchResponse) -> ResourceFetchResponse:
            existing_cursor = response.updated_state
            if self.resource_config.maybe_state_time_attribute is not None:
                params[f"{self.resource_config.maybe_state_time_attribute}[gte]"] = existing_cursor.min_time_cursor.isoformat()

            if existing_cursor.maybe_pagination_cursor is not None:
                params["cursor"] = existing_cursor.maybe_pagination_cursor

            response_json = req_session.get(
                BASE_ORB_API_URL + self.resource_config.api_path,
                headers=self.auth_header,
                params=params,
            ).json()

            resources = response_json["data"]
            updated_pagination_cursor = (
                response_json["pagination_metadata"]["next_cursor"] if response_json["pagination_metadata"]["has_more"] else None
            )
            if supports_incremental_syncs:
                updated_response_cursor = existing_cursor.with_updated_pagination_result(
                    pagination_cursor=updated_pagination_cursor,
                    max_time_in_page=max(
                        map(
                            lambda resource: cast(
                                pendulum.DateTime, pendulum.parse(resource[self.resource_config.maybe_state_time_attribute])
                            ),
                            resources,
                        ),
                        default=None,
                    ),
                )
            else:
                assert existing_cursor.min_time_cursor == pendulum.from_timestamp(0)
                assert existing_cursor.min_time_after_pagination_exhausted is None
                updated_response_cursor = replace(existing_cursor, maybe_pagination_cursor=updated_pagination_cursor)

            # Update passed in response with new resources
            for resource in resources:
                response.add_resource(resource)

            response.updated_state = updated_response_cursor
            return response

        response = ResourceFetchResponse.init_with_cursor(initial_cursor, resource_config=self.resource_config)
        updated_response = update_with_new_result_page(response)
        num_pages_fetched = 1

        # Fetch until there's nothing to paginate, or we've reached the max pages per sync
        while not updated_response.updated_state.pagination_exhausted():
            updated_response = update_with_new_result_page(response)
            num_pages_fetched += 1
            if num_pages_fetched == MAX_PAGES_PER_SYNC:
                break

        return updated_response


class NestedResourceFetcher(AbstractResourceFetcher[CursorWithSlices]):
    """
    Used to fetch resources that require a full view of a parent
    resource in order to be fetched. For example, to fetch all
    Credit Ledger Entries, we need to first fetch all customers
    """

    def fetch_slices(self, pagination_cursor: Optional[str]) -> Tuple[List[str], SimplePaginationCursor]:
        all_slices_api_path = self.resource_config.all_slices_api_path
        parent_resources, all_slices_cursor = self.fetch_resources_from_path(
            all_slices_api_path, pagination_cursor=pagination_cursor
        )
        return list(map(lambda resource: resource["id"], parent_resources)), all_slices_cursor

    def fetch_after_cursor(self, initial_cursor: CursorWithSlices) -> NestedResourceFetchResponse:
        """
        Fetches all parent slices, and then for each of those slices, advances pagination
        """
        logger.info(f"Current number of slices: {len(initial_cursor.per_slice_cursor.keys())}")
        response = NestedResourceFetchResponse.init_with_cursor(initial_cursor, resource_config=self.resource_config)

        # If we haven't finished exhausting all slices, then don't try to fetch more slices.
        paginating_existing_slices = False
        if not response.get_updated_state().all_current_slices_exhausted():
            logger.info("Continuing to fetch nested resources for the current set of slices...")
            paginating_existing_slices = True
            slice_ids = list(response.get_updated_state().per_slice_cursor.keys())
        else:
            # In the first invocation of this fetcher, we'll fetch some number of slices (falling into this block)
            # and then in subsequent pagination invocations we'll exhaust them (falling into the above conditional).
            # After the first set of slices is exhausted, we'll then fall into this block and fetch the rest of the slices.
            # Finally, we'll want to start the whole cycle again... and at that point, pagination will be exhausted because
            # we'll `all_current_slices_exhausted` and `has_more=False` on the top-level cursor.
            logger.info("All current slices are exhausted; fetching more...")
            slice_ids, all_slices_cursor = self.fetch_slices(
                pagination_cursor=response.top_level_slices_pagination_cursor().current_cursor
            )
            if all_slices_cursor.current_cursor is None and all_slices_cursor.has_more is False:
                logger.info("No more slices to fetch, so once this set of slices has been exhausted we'll start over...")
            response.update_all_slices_cursor(all_slices_cursor)

        slice_id_cursors = [(slice_id, initial_cursor.cursor_for_slice(slice_id)) for slice_id in slice_ids]
        num_slices_fetched = 0
        for (slice_id, slice_cursor) in slice_id_cursors:
            if paginating_existing_slices and slice_cursor.pagination_exhausted():
                # We can only skip exhausted slices when paginating_existing_slices, because we need to
                # go past the min/existing cursor at least once to know whether pagination is exhausted
                continue
            # Only support direct fetchers for now
            slice_resource_fetcher = DirectResourceFetcher(self.auth_header, self.resource_config.slice_resource_config(slice_id))
            fetch_response: ResourceFetchResponse = slice_resource_fetcher.fetch_after_cursor(slice_cursor)
            response.add_response_for_slice(slice_id, fetch_response)
            num_slices_fetched += 1

        logger.info(f"Fetched entries for {num_slices_fetched} slices")
        return response


class ResourceEventBasedResourceFetcher(AbstractResourceFetcher[Cursor]):
    def resource_event_resource_type_name(self):
        return self.resource_config.resultant_schema_key

    def resources_from_resource_event(self, event: Dict) -> List[Any]:
        return [event[self.resource_event_resource_type_name()]]

    def fetch_after_cursor(self, initial_cursor: Cursor) -> ResourceFetchResponse:
        """
        Fetches resources after the passed in cursor. Note that this will also attempt to paginate
        up to MAX_PAGES_PER_SYNC times.

        Since this is the `ResourceEventBased` fetcher, it queries for resource events of the corresponding
        type, and collects those serialized resources.
        """

        # If a resource does not support an incremental sync, it means that that the cursor does not store
        # any time-based boundary -- just pagination state. By consequence, an invocation of the lambda
        # might be doing an incremental *pagination* but we'll loop through the whole pagination in every
        # conceptual sync.
        supports_incremental_syncs = self.resource_config.maybe_state_time_attribute is not None

        params: Dict[str, Any] = {"limit": PAGE_SIZE, "resource_type": self.resource_event_resource_type_name()}

        def update_with_new_result_page(response: ResourceFetchResponse) -> ResourceFetchResponse:
            existing_cursor = response.updated_state

            # This is always `created_at` because we're querying over the resource events, not the resource itself
            params["created_at[gte]"] = existing_cursor.min_time_cursor.isoformat()

            if existing_cursor.maybe_pagination_cursor is not None:
                params["cursor"] = existing_cursor.maybe_pagination_cursor

            response_json = req_session.get(
                BASE_ORB_API_URL + "resource_events",
                headers=self.auth_header,
                params=params,
            ).json()

            resource_events = response_json["data"]
            updated_pagination_cursor = (
                response_json["pagination_metadata"]["next_cursor"] if response_json["pagination_metadata"]["has_more"] else None
            )
            if supports_incremental_syncs:
                updated_response_cursor = existing_cursor.with_updated_pagination_result(
                    pagination_cursor=updated_pagination_cursor,
                    max_time_in_page=max(
                        map(lambda event: cast(pendulum.DateTime, pendulum.parse(event["created_at"])), resource_events),
                        default=None,
                    ),
                )
            else:
                # If we aren't doing incremental syncs, we should *only* have pagination state and no
                # time-based state.
                assert existing_cursor.min_time_cursor == pendulum.from_timestamp(0)
                assert existing_cursor.min_time_after_pagination_exhausted is None
                updated_response_cursor = replace(existing_cursor, maybe_pagination_cursor=updated_pagination_cursor)

            # Update passed in response with new resources
            for event in resource_events:
                # e.g. event["subscription"] or event["invoice"]
                resources = self.resources_from_resource_event(event)
                for resource in resources:
                    response.add_resource(resource)

            response.updated_state = updated_response_cursor
            return response

        response = ResourceFetchResponse.init_with_cursor(initial_cursor, resource_config=self.resource_config)
        updated_response = update_with_new_result_page(response)
        num_pages_fetched = 1

        # Fetch until there's nothing to paginate, or we've reached the max pages per sync
        while not updated_response.get_updated_state().pagination_exhausted() and num_pages_fetched < MAX_PAGES_PER_SYNC:
            updated_response = update_with_new_result_page(response)
            num_pages_fetched += 1

        return updated_response


class SubscriptionVersionFetcher(ResourceEventBasedResourceFetcher):
    def resource_event_resource_type_name(self):
        return "subscription"

    def resources_from_resource_event(self, event: Dict) -> List[Any]:
        assert "subscription" in event
        subscription_id = event["subscription"]["id"]
        (subscription_versions, cursor) = self.fetch_resources_from_path(f"subscription_versions?subscription_id={subscription_id}")
        assert cursor.has_more is False
        return subscription_versions


class SubscriptionCostsFetcher(AbstractResourceFetcher[SubscriptionCostsCursor]):
    """
    Fetches subscription costs across all subscriptions. The pagination strategy is
    to pick a timeframe, and then paginate over all subscriptions for that timeframe
    and get periodic costs for them. Once all subscriptions have been exhausted for
    that timeframe, we move on to the next timeframe.

    The size of the timeframe is determined by the `COSTS_TIMEFRAME_WINDOW` constant.
    """

    def fetch_subscriptions(self, subscription_pagination_cursor: Optional[str]) -> Tuple[List[Any], SimplePaginationCursor]:
        parent_resources, all_slices_cursor = self.fetch_resources_from_path(
            "subscriptions", pagination_cursor=subscription_pagination_cursor
        )
        return parent_resources, all_slices_cursor

    def fetch_after_cursor(self, initial_cursor: SubscriptionCostsCursor) -> AbstractResourceFetchResponse[SubscriptionCostsCursor]:
        response = SubscriptionCostsFetchResponse.init_with_cursor(initial_cursor, resource_config=self.resource_config)
        timeframe_start = initial_cursor.current_timeframe_start
        timeframe_end = initial_cursor.current_timeframe_end

        (subscriptions, subscription_pagination_cursor) = self.fetch_subscriptions(
            initial_cursor.current_subscriptions_pagination_cursor
        )
        for subscription in subscriptions:
            subscription_id = subscription['id']
            if subscription["status"] == "upcoming":
                logger.info("Skipping upcoming subscription: %s", subscription_id)
                continue
            one_month_costs = req_session.get(
                BASE_ORB_API_URL + f"subscriptions/{subscription_id}/costs",
                headers=self.auth_header,
                params={
                    "timeframe_start": timeframe_start.isoformat(),
                    "timeframe_end": timeframe_end.isoformat(),
                    "view_mode": "periodic",
                },
            ).json()
            daily_costs = one_month_costs.get("data")
            if daily_costs is None:
                continue
            for cost in daily_costs:
                timeframe_start_str = cost["timeframe_start"]
                timeframe_end_str = cost["timeframe_end"]
                # Add each price's costs separately
                for price_cost in cost["per_price_costs"]:
                    psc = PeriodicSubscriptionCost(
                        subscription_id=subscription_id,
                        timeframe_start=timeframe_start_str,
                        timeframe_end=timeframe_end_str,
                        subtotal=price_cost["subtotal"],
                        total=price_cost["total"],
                        price_id=price_cost["price"]["id"],
                        grouped_costs_json=json.dumps(price_cost["price_groups"]),
                    )
                    response.add_resource(psc)

        # This will advance the timeframe if we're done with all subscriptions;
        # otherwise it will just update the pagination cursor and we'll continue on the
        # same timeframe
        response.update_cursor_state(subscriptions_cursor=subscription_pagination_cursor, timeframe_end=timeframe_end)

        return response


customer_resource_config = ResourceConfig(
    api_path="customers",
    maybe_state_time_attribute="created_at",
    resultant_schema_key="customer",
    fetch_strategy="resource_events",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
    page_size=CUSTOMER_RESOURCE_EVENTS_PAGE_SIZE,
)
plan_resource_config = ResourceConfig(
    api_path="plans",
    maybe_state_time_attribute="created_at",
    resultant_schema_key="plan",
    fetch_strategy="resource_events",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
)

issued_invoice_resource_config = ResourceConfig(
    api_path="invoices",
    # Note that
    # we don't use `created_at` for invoices because
    # there might be invoices created in the past that
    # are newly issued which we do not want to miss
    # in the query.
    maybe_state_time_attribute="invoice_date",
    resultant_schema_key="invoice",
    fetch_strategy="resource_events",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
)
draft_invoice_resource_config = ResourceConfig(
    api_path="invoices?status=draft",
    maybe_state_time_attribute=None,
    # Both draft invoices and issued invoices go into the same
    # output
    resultant_schema_key="invoice",
    fetch_strategy="direct_fetch",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
)
subscription_resource_config = ResourceConfig(
    api_path="subscriptions",
    maybe_state_time_attribute="created_at",
    resultant_schema_key="subscription",
    fetch_strategy="resource_events",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
)

subscription_version_resource_config = ResourceConfig(
    api_path="subscriptions",
    maybe_state_time_attribute="created_at",
    resultant_schema_key="subscription_version",
    fetch_strategy="resource_events",
    primary_key_list=["subscription_id", "start_date"],
    maybe_fetcher_type=SubscriptionVersionFetcher,
)

credit_ledger_entry_resource_config = NestedResourceConfig(
    all_slices_api_path="customers",
    single_slice_api_path=lambda customer_id: f"customers/{customer_id}/credits/ledger?entry_status=committed",
    maybe_state_time_attribute="created_at",
    resultant_schema_key="credit_ledger_entry",
    primary_key_list=["id"],
    maybe_fetcher_type=None,
    page_size=LEDGER_ENTRIES_PAGE_SIZE,
)

RESOURCE_TO_RESOURCE_CONFIG: Dict[str, BaseResourceConfig] = {
    "customer": customer_resource_config,
    "plan": plan_resource_config,
    "issued_invoice": issued_invoice_resource_config,
    "draft_invoice": draft_invoice_resource_config,
    "subscription": subscription_resource_config,
    "subscription_version": subscription_version_resource_config,
    "credit_ledger_entry": credit_ledger_entry_resource_config,
    "subscription_costs": BaseResourceConfig(
        primary_key_list=["subscription_id", "timeframe_start", "timeframe_end", "price_id"],
        resultant_schema_key="subscription_costs",
        maybe_fetcher_type=SubscriptionCostsFetcher,
    ),
}


def lambda_handler(req, context):
    secrets = req["secrets"]
    orb_api_key = secrets.get("orb_api_key")
    authorization_header = {"Authorization": f"Bearer {orb_api_key}"}

    if req.get("setup_test"):
        # This is a test invocation, so don't actually do any sync.
        req_session.get(BASE_ORB_API_URL + "ping", headers=authorization_header)
        return FivetranFunctionResponse.test_response()

    state = FunctionState.from_serialized_state(req.get("state"))

    has_more = False
    collected_resources: Dict = {}

    ## There are two types of invocations;
    ## 1. Fivetran is invoking this function on a schedule.
    ##    In this case, we expect all the resources to effectively run a full "conceptual sync"
    ##    from their stored state. In most cases, the stored state will include a time boundary
    ##    so we'll be adding resources after that time boundary. There's some cases for which that's
    ##    not true, like draft invoices -- there, we'll be syncing all draft invoices in every
    ##    incremental sync.
    ## 2. Fivetran is invoking this function immediately after the last invocation, because
    ##    the last invocation didn't "finish". Here, we're conceptually still in the same sync
    ##    but some subset of streams might have more to output. We can know this is true if
    ##    any of the streams have a `pagination_exhausted` value of `False`.

    incomplete_pagination_resources = set()
    is_paginated_invocation = False
    for resource, resource_config in RESOURCE_TO_RESOURCE_CONFIG.items():
        state_cursor = state.state_cursor_for_resource(resource)
        if not state_cursor.pagination_exhausted():
            is_paginated_invocation = True
            incomplete_pagination_resources.add(resource)

    streams_to_invoke = incomplete_pagination_resources if is_paginated_invocation else set(RESOURCE_TO_RESOURCE_CONFIG.keys())
    for resource, resource_config in RESOURCE_TO_RESOURCE_CONFIG.items():
        if resource not in streams_to_invoke:
            logger.info(f"Skipping {resource} resource because this is a paginated invocation, and this resource is exhausted.")
            continue
        fetcher_type = fetcher_for_resource_config(resource_config)
        fetcher: AbstractResourceFetcher = fetcher_type(authorization_header=authorization_header, resource_config=resource_config)
        fetch_response: AbstractResourceFetchResponse = fetcher.fetch_all(
            maybe_existing_state_cursor=state.state_cursor_for_resource(resource)
        )

        # If pagination isn't exhausted for *any* resource, keep invoking immediately.
        # This next invocation would be a "paginated invocation" and should only continue collecting
        # resources for the unexhausted streams.
        if not fetch_response.get_updated_state().pagination_exhausted():
            has_more = True
        output_resources = collected_resources.get(resource_config.resultant_schema_key, [])
        all_resources = list(fetch_response.all_resources())
        logger.info(f"Collected {len(all_resources)} {resource_config.resultant_schema_key} resources.")
        output_resources.extend(all_resources)
        collected_resources[resource_config.resultant_schema_key] = output_resources
        state.set_for_resource(resource, fetch_response.get_updated_state())

    maybe_s3_bucket = req.get("bucket")
    maybe_s3_file = req.get("file")
    s3_sync = False

    function_response = FivetranFunctionResponse(state=state, insert=collected_resources, has_more=has_more)

    if maybe_s3_bucket and maybe_s3_file:
        s3_sync = True
        # Write the output to s3
        s3_client.put_object(
            Body=(bytes(json.dumps(function_response.serialize_output_to_s3()).encode("UTF-8"))),
            Bucket=maybe_s3_bucket,
            Key=maybe_s3_file,
        )

    return function_response.as_dict(s3_sync=s3_sync)
