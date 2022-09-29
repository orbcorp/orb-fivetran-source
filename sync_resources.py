from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pendulum
import requests

BASE_ORB_API_URL = "https://api.billwithorb.com/v1/"
PAGE_SIZE = 20


@dataclass(frozen=True)
class ResourceConfig:
    api_path: str
    # This determines what API field we use
    # to store state in between syncs.
    state_cursor_field: str
    should_store_state: bool
    resultant_schema_key: str


customer_resource_config = ResourceConfig(
    api_path="customers",
    state_cursor_field="created_at",
    should_store_state=True,
    resultant_schema_key="customer",
)
plan_resource_config = ResourceConfig(
    api_path="plans",
    state_cursor_field="created_at",
    should_store_state=True,
    resultant_schema_key="plan",
)

issued_invoice_resource_config = ResourceConfig(
    api_path="invoices",
    # Note that
    # we don't use `created_at` for invoices because
    # there might be invoices created in the past that
    # are newly issued which we do not want to miss
    # in the query.
    state_cursor_field="invoice_date",
    should_store_state=True,
    resultant_schema_key="invoice",
)
draft_invoice_resource_config = ResourceConfig(
    api_path="invoices?status=draft",
    state_cursor_field="invoice_date",
    should_store_state=False,
    # Both draft invoices and issued invoices go into the same
    # output
    resultant_schema_key="invoice",
)
subscription_resource_config = ResourceConfig(
    api_path="subscriptions",
    state_cursor_field="created_at",
    should_store_state=True,
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
    customer_created_at_cursor: datetime
    plan_created_at_cursor: datetime
    # This cursor only applies to issued invoices.
    # We always sync all draft invoices available.
    invoices_invoice_date_cursor: datetime
    subscription_created_at_cursor: datetime

    def as_dict(self):
        return {
            "customer_created_at_cursor": self.customer_created_at_cursor.isoformat(),
            "plan_created_at_cursor": self.plan_created_at_cursor.isoformat(),
            "invoices_invoice_date_cursor": self.invoices_invoice_date_cursor.isoformat(),
            "subscription_created_at_cursor": self.subscription_created_at_cursor.isoformat(),
        }

    @staticmethod
    def from_serialized_state(state: Optional[Dict]):
        if state is None:
            return FunctionState(
                customer_created_at_cursor=pendulum.DateTime.min,
                plan_created_at_cursor=pendulum.DateTime.min,
                invoices_invoice_date_cursor=pendulum.DateTime.min,
                subscription_created_at_cursor=pendulum.DateTime.min,
            )

        customer_created_at_cursor = state.get("customer_created_at_cursor")
        plan_created_at_cursor = state.get("plan_created_at_cursor")
        invoices_invoice_date_cursor = state.get("invoices_invoice_date_cursor")
        subscription_created_at_cursor = state.get("subscription_created_at_cursor")

        return FunctionState(
            customer_created_at_cursor=pendulum.parse(customer_created_at_cursor)
            if customer_created_at_cursor is not None
            else pendulum.DateTime.min,
            plan_created_at_cursor=pendulum.parse(plan_created_at_cursor)
            if plan_created_at_cursor is not None
            else pendulum.DateTime.min,
            invoices_invoice_date_cursor=pendulum.parse(invoices_invoice_date_cursor)
            if invoices_invoice_date_cursor is not None
            else pendulum.DateTime.min,
            subscription_created_at_cursor=pendulum.parse(
                subscription_created_at_cursor
            )
            if subscription_created_at_cursor is not None
            else pendulum.DateTime.min,
        )

    def state_cursor_for_resource(self, resource) -> datetime:
        assert resource in RESOURCE_TO_RESOURCE_CONFIG.keys()
        if resource == "customer":
            return self.customer_created_at_cursor
        elif resource == "plan":
            return self.plan_created_at_cursor
        elif resource == "subscription":
            return self.subscription_created_at_cursor
        elif resource == "issued_invoice":
            return self.invoices_invoice_date_cursor
        else:
            raise Exception(f"Cannot get state for resource {resource}")

    def set_for_resource(self, resource: str, cursor: datetime) -> None:
        assert resource in RESOURCE_TO_RESOURCE_CONFIG.keys()
        if resource == "subscription":
            self.subscription_created_at_cursor = cursor
        elif resource == "customer":
            self.customer_created_at_cursor = cursor
        elif resource == "plan":
            self.plan_created_at_cursor = cursor
        elif resource == "issued_invoice":
            self.invoices_invoice_date_cursor = cursor
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
    updated_state: datetime


class ResourceFetcher:
    """
    Note that this is a paginated resource fetcher, but a given invocation
    of this serverless function will attempt to exhaust pagination. This will mean
    a hefty first invocation (when state is empty), but the incremental nature of this
    will mean subsequent syncs are still fast.
    """

    def __init__(self, authorization_header, resource_path, cursor_field_name):
        self.auth_header = authorization_header
        self.resource_path = resource_path
        self.cursor_field_name = cursor_field_name

    def fetch_all(
        self, existing_state_cursor: Optional[datetime]
    ) -> ResourceFetchResponse:
        existing_state_cursor = (
            pendulum.DateTime.min
            if existing_state_cursor is None
            else existing_state_cursor
        )
        collected_resources = []
        response: ResourceSingleFetchResponse = self.fetch(
            existing_state_cursor, pagination_cursor=None
        )
        collected_resources.extend(response.resources)

        while response.has_more:
            response = self.fetch(
                state_cursor=existing_state_cursor, pagination_cursor=response.cursor
            )
            collected_resources.extend(response.resources)

        max_cursor_field_from_response = max(
            [
                pendulum.parse(resource[self.cursor_field_name])
                for resource in collected_resources
            ],
            default=pendulum.DateTime.min,
        )
        updated_state = (
            max(max_cursor_field_from_response, existing_state_cursor)
            if max_cursor_field_from_response is not None
            else existing_state_cursor
        )

        return ResourceFetchResponse(
            resources=collected_resources, updated_state=updated_state
        )

    def fetch(
        self, state_cursor: Optional[datetime], pagination_cursor: Optional[str]
    ) -> ResourceSingleFetchResponse:
        params: Dict[str, Any] = {"limit": PAGE_SIZE}
        if state_cursor is not None:
            params[f"{self.cursor_field_name}[gte]"] = state_cursor.isoformat()

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

    collected_resources: Dict = {}
    for resource, resource_config in RESOURCE_TO_RESOURCE_CONFIG.items():
        fetcher = ResourceFetcher(
            authorization_header=authorization_header,
            resource_path=resource_config.api_path,
            cursor_field_name=resource_config.state_cursor_field,
        )
        fetch_response: ResourceFetchResponse = fetcher.fetch_all(
            existing_state_cursor=state.state_cursor_for_resource(resource)
            if resource_config.should_store_state
            else None
        )
        output_resources = collected_resources.get(
            resource_config.resultant_schema_key, []
        )
        output_resources.extend(fetch_response.resources)
        collected_resources[resource_config.resultant_schema_key] = output_resources
        if resource_config.should_store_state:
            state.set_for_resource(resource, fetch_response.updated_state)

    return FivetranFunctionResponse(state=state, insert=collected_resources).as_dict()
