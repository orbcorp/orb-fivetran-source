from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pendulum
import requests

BASE_ORB_API_URL = "https://api.billwithorb.com/v1/"
PAGE_SIZE = 20
MAX_PAGES_PER_SYNC = 5


@dataclass(frozen=True)
class Cursor:
    time_cursor: datetime
    maybe_pagination_cursor: Optional[str]


@dataclass(frozen=True)
class ResourceConfig:
    api_path: str
    # This determines what API field we use
    # to store state in between syncs.
    maybe_state_cursor_field: Optional[str]
    resultant_schema_key: str


customer_resource_config = ResourceConfig(
    api_path="customers",
    maybe_state_cursor_field="created_at",
    resultant_schema_key="customer",
)
plan_resource_config = ResourceConfig(
    api_path="plans",
    maybe_state_cursor_field="created_at",
    resultant_schema_key="plan",
)

issued_invoice_resource_config = ResourceConfig(
    api_path="invoices",
    # Note that
    # we don't use `created_at` for invoices because
    # there might be invoices created in the past that
    # are newly issued which we do not want to miss
    # in the query.
    maybe_state_cursor_field="invoice_date",
    resultant_schema_key="invoice",
)
draft_invoice_resource_config = ResourceConfig(
    api_path="invoices?status=draft",
    maybe_state_cursor_field=None,
    # Both draft invoices and issued invoices go into the same
    # output
    resultant_schema_key="invoice",
)
subscription_resource_config = ResourceConfig(
    api_path="subscriptions",
    maybe_state_cursor_field="created_at",
    resultant_schema_key="subscription",
)

RESOURCE_TO_RESOURCE_CONFIG = {
    "customer": customer_resource_config,
    "plan": plan_resource_config,
    "issued_invoice": issued_invoice_resource_config,
    "draft_invoice": draft_invoice_resource_config,
    "subscription": subscription_resource_config,
}


@dataclass
class FunctionState:
    customer_cursor: Cursor
    plan_cursor: Cursor
    # This cursor only applies to issued invoices.
    # We always sync all draft invoices available.
    invoice_cursor: Cursor
    draft_invoice_cursor: Cursor
    subscription_cursor: Cursor

    def as_dict(self):
        return {
            "customer_created_at_cursor": self.customer_cursor.time_cursor.isoformat(),
            "customer_pagination_cursor": self.customer_cursor.maybe_pagination_cursor,
            "plan_created_at_cursor": self.plan_cursor.time_cursor.isoformat(),
            "plan_pagination_cursor": self.plan_cursor.maybe_pagination_cursor,
            "invoices_invoice_date_cursor": self.invoice_cursor.time_cursor.isoformat(),
            "invoices_pagination_cursor": self.invoice_cursor.maybe_pagination_cursor,
            # Draft invoices don't have a time cursor
            "draft_invoices_pagination_cursor": self.draft_invoice_cursor.maybe_pagination_cursor,
            "subscription_created_at_cursor": self.subscription_cursor.time_cursor.isoformat(),
            "subscription_pagination_cursor": self.subscription_cursor.maybe_pagination_cursor,
        }

    @staticmethod
    def from_serialized_state(state: Optional[Dict]):
        if state is None:
            return FunctionState(
                customer_cursor=Cursor(pendulum.DateTime.min, None),
                plan_cursor=Cursor(pendulum.DateTime.min, None),
                invoice_cursor=Cursor(pendulum.DateTime.min, None),
                draft_invoice_cursor=Cursor(pendulum.DateTime.min, None),
                subscription_cursor=Cursor(pendulum.DateTime.min, None),
            )

        customer_created_at_cursor = state.get("customer_created_at_cursor")
        plan_created_at_cursor = state.get("plan_created_at_cursor")
        invoices_invoice_date_cursor = state.get("invoices_invoice_date_cursor")
        subscription_created_at_cursor = state.get("subscription_created_at_cursor")
        customer_pagination_cursor = state.get("customer_pagination_cursor")
        plan_pagination_cursor = state.get("plan_pagination_cursor")
        invoices_pagination_cursor = state.get("invoices_pagination_cursor")
        draft_invoices_pagination_cursor = state.get("draft_invoices_pagination_cursor")
        subscription_pagination_cursor = state.get("subscription_pagination_cursor")

        return FunctionState(
            customer_cursor=Cursor(
                time_cursor=pendulum.parse(customer_created_at_cursor)
                if customer_created_at_cursor is not None
                else pendulum.DateTime.min,
                maybe_pagination_cursor=customer_pagination_cursor,
            ),
            plan_cursor=Cursor(
                time_cursor=pendulum.parse(plan_created_at_cursor)
                if plan_created_at_cursor is not None
                else pendulum.DateTime.min,
                maybe_pagination_cursor=plan_pagination_cursor,
            ),
            invoice_cursor=Cursor(
                time_cursor=pendulum.parse(invoices_invoice_date_cursor)
                if invoices_invoice_date_cursor is not None
                else pendulum.DateTime.min,
                maybe_pagination_cursor=invoices_pagination_cursor,
            ),
            subscription_cursor=Cursor(
                time_cursor=pendulum.parse(subscription_created_at_cursor)
                if subscription_created_at_cursor is not None
                else pendulum.DateTime.min,
                maybe_pagination_cursor=subscription_pagination_cursor,
            ),
            draft_invoice_cursor=Cursor(
                time_cursor=pendulum.DateTime.min,
                maybe_pagination_cursor=draft_invoices_pagination_cursor,
            ),
        )

    def state_cursor_for_resource(self, resource) -> Cursor:
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

        else:
            raise Exception(f"Cannot get state for resource {resource}")

    def set_for_resource(self, resource: str, cursor: Cursor) -> None:
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
        else:
            raise Exception(f"Cannot set state for resource {resource}")


@dataclass
class FivetranFunctionResponse:
    state: FunctionState
    insert: Dict = field(default_factory=dict)
    schema: Dict = field(
        default_factory=lambda: {
            "customer": {"primary_key": ["id"]},
            "plan": {"primary_key": ["id"]},
            "invoice": {"primary_key": ["id"]},
            "subscription": {"primary_key": ["id"]},
        }
    )
    has_more: bool = False

    def as_dict(self):
        return {
            "state": self.state.as_dict(),
            "insert": self.insert,
            "delete": {},
            "schema": self.schema,
            "hasMore": self.has_more,
        }

    @staticmethod
    def test_response():
        return FivetranFunctionResponse(
            state={}, insert={}, schema={}, has_more={}
        ).as_dict()


@dataclass
class ResourceSingleFetchResponse:
    resources: List[Any]
    has_more: bool
    cursor: Optional[str]


@dataclass
class ResourceFetchResponse:
    resources: List[Any]
    updated_state: Cursor


class ResourceFetcher:
    """
    Note that this is a paginated resource fetcher, but a given invocation
    of this serverless function will attempt to exhaust pagination. This will mean
    a hefty first invocation (when state is empty), but the incremental nature of this
    will mean subsequent syncs are still fast.
    """

    def __init__(self, authorization_header, resource_path, maybe_cursor_field_name):
        self.auth_header = authorization_header
        self.resource_path = resource_path
        self.maybe_cursor_field_name = maybe_cursor_field_name

    def fetch_all(
        self, existing_state_cursor: Optional[Cursor]
    ) -> ResourceFetchResponse:
        time_cursor = (
            pendulum.DateTime.min
            if existing_state_cursor is None
            else existing_state_cursor.time_cursor
        )
        maybe_pagination_cursor = (
            None
            if existing_state_cursor is None
            else existing_state_cursor.maybe_pagination_cursor
        )
        collected_resources = []
        response: ResourceSingleFetchResponse = self.fetch(
            time_cursor=time_cursor, pagination_cursor=maybe_pagination_cursor
        )
        maybe_pagination_cursor = response.cursor
        num_pages_fetched = 1
        collected_resources.extend(response.resources)

        while (
            maybe_pagination_cursor is not None
            and num_pages_fetched < MAX_PAGES_PER_SYNC
        ):
            response = self.fetch(
                time_cursor=time_cursor,
                pagination_cursor=maybe_pagination_cursor,
            )
            collected_resources.extend(response.resources)
            maybe_pagination_cursor = response.cursor
            num_pages_fetched += 1

        if maybe_pagination_cursor is None and self.maybe_cursor_field_name is not None:
            max_cursor_field_from_response = max(
                [
                    pendulum.parse(resource[self.maybe_cursor_field_name])
                    for resource in collected_resources
                ],
                default=pendulum.DateTime.min,
            )
            time_cursor = (
                max(max_cursor_field_from_response, time_cursor)
                if max_cursor_field_from_response is not None
                else time_cursor
            )

        updated_state = Cursor(
            time_cursor=time_cursor,
            maybe_pagination_cursor=maybe_pagination_cursor,
        )

        return ResourceFetchResponse(
            resources=collected_resources, updated_state=updated_state
        )

    def fetch(
        self, time_cursor: Optional[datetime], pagination_cursor: Optional[str]
    ) -> ResourceSingleFetchResponse:
        params: Dict[str, Any] = {"limit": PAGE_SIZE}
        if time_cursor is not None and self.maybe_cursor_field_name is not None:
            params[f"{self.maybe_cursor_field_name}[gte]"] = time_cursor.isoformat()

        if pagination_cursor is not None:
            params["cursor"] = pagination_cursor

        response_json = requests.get(
            BASE_ORB_API_URL + self.resource_path,
            headers=self.auth_header,
            params=params,
        ).json()

        return ResourceSingleFetchResponse(
            resources=response_json["data"],
            has_more=response_json["pagination_metadata"]["has_more"],
            cursor=response_json["pagination_metadata"]["next_cursor"],
        )


def lambda_handler(req, context):
    secrets = req["secrets"]
    orb_api_key = secrets.get("orb_api_key")
    authorization_header = {"Authorization": f"Bearer {orb_api_key}"}

    if req.get("setup_test"):
        # This is a test invocation, so don't actually do any sync.
        requests.get(BASE_ORB_API_URL + "ping", headers=authorization_header)
        return FivetranFunctionResponse.test_response()

    state = FunctionState.from_serialized_state(req.get("state"))

    has_more = False
    collected_resources: Dict = {}
    for resource, resource_config in RESOURCE_TO_RESOURCE_CONFIG.items():
        fetcher = ResourceFetcher(
            authorization_header=authorization_header,
            resource_path=resource_config.api_path,
            maybe_cursor_field_name=resource_config.maybe_state_cursor_field,
        )
        fetch_response: ResourceFetchResponse = fetcher.fetch_all(
            existing_state_cursor=state.state_cursor_for_resource(resource)
        )
        if fetch_response.updated_state.maybe_pagination_cursor is not None:
            has_more = True
        output_resources = collected_resources.get(
            resource_config.resultant_schema_key, []
        )
        output_resources.extend(fetch_response.resources)
        collected_resources[resource_config.resultant_schema_key] = output_resources
        state.set_for_resource(resource, fetch_response.updated_state)

    return FivetranFunctionResponse(
        state=state, insert=collected_resources, has_more=has_more
    ).as_dict()
