"""REST client base classes for Marketo streams."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk.streams import RESTStream
from memoization import cached

from tap_marketo.auth import MarketoAuthenticator


class MarketoStream(RESTStream):
    """Base stream for Marketo REST endpoints."""

    primary_keys = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.result[*]"
    next_page_token_jsonpath = "$.nextPageToken"


    @property
    def schema(self) -> dict:
        return self.get_schema()

    @property
    def default_headers(self) -> dict:
        return {
            **self.authenticator.auth_headers
        }

    @property
    def url_base(self) -> str:
        """Return API base URL from config."""
        return self.config["base_url"].rstrip("/") + "/"

    @property
    @cached
    def authenticator(self) -> MarketoAuthenticator:
        """Return stream authenticator."""
        return MarketoAuthenticator.create_for_stream(self)

    def parse_response(self, response):
        """Validate Marketo success field before parsing records."""
        payload = response.json()
        if payload.get("success") is False:
            errors = payload.get("errors") or payload
            raise RuntimeError(f"Marketo API error for stream '{self.name}': {errors}")
        return super().parse_response(response)

    def get_next_page_token(self, response, previous_token: Any):
        """Use nextPageToken only if moreResult is true."""
        payload = response.json()
        if not payload.get("moreResult"):
            return None
        return payload.get("nextPageToken")
