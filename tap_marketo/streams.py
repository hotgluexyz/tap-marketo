"""Stream definitions for tap-marketo."""

from __future__ import annotations

from typing import Any, Dict
from urllib.parse import urljoin

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


class ActivityTypesHelperStream(MarketoStream):
    name = "types_helper"
    path = "rest/v1/activities/types.json"
    def get_schema(self):
        return {
            "properties": {}
        }


class LeadsStream(MarketoStream):
    """Leads stream using Get Leads by Filter Type API."""

    name = "leads"
    path = "rest/v1/leads.json"
    replication_key = "updatedAt"
    primary_keys = ["id"]

    describe_path = "rest/v1/leads/describe.json"
    bulk_export_create_path = "bulk/v1/leads/export/create.json"
    bulk_export_enqueue_path = "bulk/v1/leads/export/{job_id}/enqueue.json"
    bulk_export_status_path = "bulk/v1/leads/export/{job_id}/status.json"
    bulk_export_results_path = "bulk/v1/leads/export/{job_id}/file.json"

    @cached
    def get_schema(self) -> dict:
        """Build JSON schema from Marketo lead describe endpoint."""
        base_url = self.config["base_url"].rstrip("/") + "/"
        describe_url = urljoin(base_url, self.describe_path)
        response = self.make_request(method="GET", url=describe_url)
        payload = response.json()

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

    def get_async_job_payload(self, context) -> dict:
        """Return payload used by create bulk export job endpoint."""

        fields = list(self.schema.get("properties", {}).keys())

        payload = {
            "format": "CSV",
            "fields": fields,
            "filter": {
                self.replication_key: {
                    "startAt": context["window_start_date"],
                    "endAt": context["window_end_date"],
                }
            },
        }
        payload.update(context)
        return payload


class ActivityTypeStream(MarketoStream):
    """Bulk export stream for a single Marketo activity type."""

    path = "rest/v1/activities.json"
    replication_key = "activityDate"
    primary_keys = ["marketoGUID"]

    bulk_export_create_path = "bulk/v1/activities/export/create.json"
    bulk_export_enqueue_path = "bulk/v1/activities/export/{job_id}/enqueue.json"
    bulk_export_status_path = "bulk/v1/activities/export/{job_id}/status.json"
    bulk_export_results_path = "bulk/v1/activities/export/{job_id}/file.json"

    def __init__(
        self,
        activity_type_id: int,
        activity_type_name: str,
        raw_schema: dict,
        primary_keys: list[dict],
        *args,
        **kwargs,
    ):
        self.activity_type_id = int(activity_type_id)
        self.name = "event_" + activity_type_name.replace(" ", "_")
        super().__init__(*args, **kwargs)

    def get_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "marketoGUID": {"type": ["null", "string"],},
                "leadId": {"type": ["null", "string"]},
                "activityDate": {"type": ["null", "string"], "format": "date-time"},
                "activityTypeId": {"type": ["null", "string"]},
                "campaignId": {"type": ["null", "string"]},
                "primaryAttributeValueId": {"type": ["null", "string"]},
                "primaryAttributeValue": {"type": ["null", "string"]},
                "actionResult": {"type": ["null", "string"]},
            },
        }

    def get_async_job_payload(self, context) -> dict:
        """Return payload used by create bulk export job endpoint."""

        payload = {
            "format": "CSV",
            "fields": [
                    "marketoGUID",
                    "leadId",
                    "activityDate",
                    "activityTypeId",
                    "campaignId",
                    "primaryAttributeValueId",
                    "primaryAttributeValue",
                    "actionResult",
                ],
            "filter": {
                "createdAt": {
                    "startAt": context["window_start_date"],
                    "endAt": context["window_end_date"],
                },
                "activityTypeIds": [self.activity_type_id],
            }
        }
        payload.update(context)
        return payload
