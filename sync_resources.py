from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pendulum
import requests

BASE_ORB_API_URL = "https://api.billwithorb.com/v1/"
PAGE_SIZE = 50

RESOURCE_TO_API_PATH = {
    "customer": "customers",
    "plan": "plans",
    "invoice": "invoices",
    "subscription": "subscriptions",
}


@dataclass
class FunctionState:
    customer_created_at_cursor: datetime
    plan_created_at_cursor: datetime
    invoices_created_at_cursor: datetime
    subscription_created_at_cursor: datetime

    def as_dict(self):
        return {
            "customer_created_at_cursor": self.customer_created_at_cursor.isoformat(),
            "plan_created_at_cursor": self.plan_created_at_cursor.isoformat(),
            "invoices_created_at_cursor": self.invoices_created_at_cursor.isoformat(),
            "subscription_created_at_cursor": self.subscription_created_at_cursor.isoformat(),
        }

    @staticmethod
    def from_serialized_state(state: Optional[Dict]):
        if state is None:
            return FunctionState(
                customer_created_at_cursor=pendulum.DateTime.min,
                plan_created_at_cursor=pendulum.DateTime.min,
                invoices_created_at_cursor=pendulum.DateTime.min,
                subscription_created_at_cursor=pendulum.DateTime.min,
            )

        customer_created_at_cursor = state.get("customer_created_at_cursor")
        plan_created_at_cursor = state.get("plan_created_at_cursor")
        invoices_created_at_cursor = state.get("invoices_created_at_cursor")
        subscription_created_at_cursor = state.get("subscription_created_at_cursor")

        return FunctionState(
            customer_created_at_cursor=pendulum.parse(customer_created_at_cursor)
            if customer_created_at_cursor is not None
            else pendulum.DateTime.min,
            plan_created_at_cursor=pendulum.parse(plan_created_at_cursor)
            if plan_created_at_cursor is not None
            else pendulum.DateTime.min,
            invoices_created_at_cursor=pendulum.parse(invoices_created_at_cursor)
            if plan_created_at_cursor is not None
            else pendulum.DateTime.min,
            subscription_created_at_cursor=pendulum.parse(
                subscription_created_at_cursor
            )
            if subscription_created_at_cursor is not None
            else pendulum.DateTime.min,
        )

    def state_cursor_for_resource(self, resource) -> datetime:
        if resource == "customer":
            return self.customer_created_at_cursor
        elif resource == "plan":
            return self.plan_created_at_cursor
        elif resource == "subscription":
            return self.subscription_created_at_cursor
        else:
            return self.invoices_created_at_cursor

    def set_for_resource(self, resource: str, cursor: datetime) -> None:
        if resource == "subscription":
            self.subscription_created_at_cursor = cursor
        if resource == "customer":
            self.customer_created_at_cursor = cursor
        elif resource == "plan":
            self.plan_created_at_cursor = cursor
        elif resource == "subscription":
            self.invoices_created_at_cursor = cursor


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

    def __init__(self, authorization_header, resource_path):
        self.auth_header = authorization_header
        self.resource_path = resource_path

    def fetch_all(self, min_created_at: Optional[datetime]) -> ResourceFetchResponse:
        collected_resources = []
        response: ResourceSingleFetchResponse = self.fetch(
            min_created_at, pagination_cursor=None
        )
        collected_resources.extend(response.resources)

        while response.has_more:
            response = self.fetch(
                min_created_at=min_created_at, pagination_cursor=response.cursor
            )
            collected_resources.extend(response.resources)

        max_created_at = max(
            [
                pendulum.parse(resource["created_at"])
                for resource in collected_resources
            ],
            default=pendulum.DateTime.min,
        )
        updated_state = (
            max(max_created_at, min_created_at)
            if max_created_at is not None
            else min_created_at
        )

        return ResourceFetchResponse(
            resources=collected_resources, updated_state=updated_state
        )

    def fetch(
        self, min_created_at: Optional[datetime], pagination_cursor: Optional[str]
    ) -> ResourceSingleFetchResponse:
        params: Dict[str, Any] = {"limit": PAGE_SIZE}
        if min_created_at is not None:
            params["created_at[gte]"] = min_created_at.isoformat()

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
    for resource, resource_path in RESOURCE_TO_API_PATH.items():
        fetcher = ResourceFetcher(
            authorization_header=authorization_header, resource_path=resource_path
        )
        fetch_response: ResourceFetchResponse = fetcher.fetch_all(
            min_created_at=state.state_cursor_for_resource(resource)
        )
        collected_resources[resource] = fetch_response.resources
        state.set_for_resource(resource, fetch_response.updated_state)

    return FivetranFunctionResponse(state=state, insert=collected_resources).as_dict()
