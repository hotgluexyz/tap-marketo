"""Stream definitions for tap-marketo."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests
from memoization import cached

from tap_marketo.client import MarketoStream


TYPE_MAP = {
    "string": {"type": ["null", "string"]},
    "email": {"type": ["null", "string"]},
    "url": {"type": ["null", "string"]},
    "phone": {"type": ["null", "string"]},
    "textarea": {"type": ["null", "string"]},
    "integer": {"type": ["null", "integer"]},
    "float": {"type": ["null", "number"]},
    "currency": {"type": ["null", "number"]},
    "score": {"type": ["null", "number"]},
    "boolean": {"type": ["null", "boolean"]},
    "date": {"type": ["null", "string"], "format": "date-time"},
    "datetime": {"type": ["null", "string"], "format": "date-time"},
}


class LeadsStream(MarketoStream):
    """Leads stream using Get Leads by Filter Type API."""

    name = "leads"
    path = "rest/v1/leads.json"
    replication_key = "updatedAt"
    primary_keys = ["id"]

    describe_path = "rest/v1/leads/describe.json"
    batch_size = 300


    def get_schema(self) -> dict:
        """Build JSON schema from Marketo lead describe endpoint."""
        base_url = self.config["base_url"].rstrip("/") + "/"
        describe_url = urljoin(base_url, self.describe_path)
        response = requests.get(describe_url, headers=self.default_headers)
        payload = response.json()

        if payload.get("success") is False:
            errors = payload.get("errors") or payload
            raise RuntimeError(f"Marketo describe API error: {errors}")

        properties: Dict[str, Any] = {}
        for field in payload.get("result", []):
            name = field.get("rest", {}).get("name")
            data_type = field.get("dataType", "string")
            if name:
                properties[name] = TYPE_MAP.get(
                    data_type.lower(), {"type": ["null", "string"]}
                )
        return {
            "type": "object",
            "properties": properties,
            "additionalProperties": True,
        }



    def get_url_params(
        self,
        context: Optional[dict],
        next_page_token: Optional[Any],
    ) -> Dict[str, Any]:
        """Return request params for Marketo leads endpoint."""
        params: Dict[str, Any] = {
            "batchSize": self.batch_size
        }

        if next_page_token:
            params["nextPageToken"] = next_page_token
            return params

        # Incremental filter on lead last-modified timestamp.
        params["filterType"] = "updatedAt"
        params["filterValues"] = "2026-02-20T00:00:00Z"
        return params
