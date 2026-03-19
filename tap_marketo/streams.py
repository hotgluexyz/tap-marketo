"""Stream definitions for tap-marketo."""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from memoization import cached

from tap_marketo.client import MarketoAsyncRESTStream, MarketoRESTStream
import copy

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


class ActivityTypesHelperStream(MarketoAsyncRESTStream):
    name = "types_helper"
    path = "rest/v1/activities/types.json"
    def get_schema(self):
        return {
            "properties": {}
        }


class LeadsStream(MarketoAsyncRESTStream):
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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object for a given record."""
        return {
            "externalCompanyId": record["externalCompanyId"],
        }    

class CompaniesStream(MarketoRESTStream):
    """Companies stream; synced as child of LeadsStream by externalCompanyId."""

    name = "companies"
    path = "rest/v1/companies.json"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = LeadsStream
    state_partitioning_keys = []

    describe_path = "rest/v1/companies/describe.json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._synced_external_company_ids: set = set()

    @cached
    def get_schema(self) -> dict:
        """Build JSON schema from Marketo companies describe endpoint."""
        base_url = self.config["base_url"].rstrip("/") + "/"
        describe_url = urljoin(base_url, self.describe_path)
        prepared = self.build_prepared_request(
            method="GET",
            url=describe_url,
        )
        response = self.request_decorator(self._request)(prepared, None)
        payload = response.json()

        result_list = payload.get("result", [])
        if not result_list:
            return {"type": "object", "properties": {},}

        properties: Dict[str, Any] = {}
        for field in result_list[0].get("fields", []):
            name = field.get("name")
            data_type = field.get("dataType", "string")
            if name:
                properties[name] = TYPE_MAP.get(
                    data_type.lower(), {"type": ["null", "string"]}
                )
        return {
            "type": "object",
            "properties": properties,
        }
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any] = None
    ) -> dict:
        params: Dict[str, Any] = {}
        externalCompanyId = context and context.get("externalCompanyId")

        params["filterType"] = "externalCompanyId"
        params["filterValues"] = int(externalCompanyId)
        props = self.schema.get("properties") or {}
        if props:
            params["fields"] = ",".join(props.keys())

        if next_page_token:
            raise NotImplementedError("Pagination is not necessary for child stream since it's a single record")
        return params
    
    def get_records(self, context):
        externalCompanyId = (context or {}).get("externalCompanyId")
        if not externalCompanyId or externalCompanyId in self._synced_external_company_ids:
            return
        self._synced_external_company_ids.add(externalCompanyId)
        yield from super().get_records(context)
    

class NamedAccountListsStream(MarketoRESTStream):

    name = "named_Account_Lists"
    path = "rest/v1/namedAccountLists.json"
    primary_keys = ["marketoGUID"]
    replication_key = None

    @cached
    def get_schema(self) -> dict:
        """No describe endpoint for named account lists; schema from API shape."""
        return {
            "type": "object",
            "properties": {
                "seq": {"type": ["null", "integer"]},
                "marketoGUID": {"type": ["null", "string"]},
                "name": {"type": ["null", "string"]},
                "type": {"type": ["null", "string"]},
                "updateable": {"type": ["null", "boolean"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
            },
        }
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any] = None
    ) -> dict:
        params: Dict[str, Any] = {}

        if next_page_token:
            params["nextPageToken"] = next_page_token
        return params
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object for a given record."""
        return {
            "named_account_list_guid": record["marketoGUID"],
        }    


class NamedAccountsStream(MarketoRESTStream):
    """Named Account stream; synced as child of Named Account Lists by marketoGUID."""
    
    name = "named_Accounts"
    path = "rest/v1/namedaccounts.json"
    primary_keys = ["marketoGUID"]
    replication_key = None
    state_partitioning_keys = []
    parent_stream_type = NamedAccountListsStream
    describe_path = "rest/v1/namedaccounts/describe.json"

    @cached
    def get_schema(self) -> dict:
        """Build JSON schema from Marketo companies describe endpoint."""
        _EXCLUDED_NAMED_ACCOUNT_FIELDS = {"crmIsDeleted"}
        base_url = self.config["base_url"].rstrip("/") + "/"
        describe_url = urljoin(base_url, self.describe_path)
        prepared = self.build_prepared_request(
            method="GET",
            url=describe_url,
        )
        response = self.request_decorator(self._request)(prepared, None)
        payload = response.json()

        result_list = payload.get("result", [])
        if not result_list:
            return {"type": "object", "properties": {},}

        properties: Dict[str, Any] = {}
        for field in result_list[0].get("fields", []):
            name = field.get("name")
            data_type = field.get("dataType", "string")
            if name and name not in _EXCLUDED_NAMED_ACCOUNT_FIELDS:
                properties[name] = TYPE_MAP.get(
                    data_type.lower(), {"type": ["null", "string"]}
                )
        return {
            "type": "object",
            "properties": properties,
        }
    
    
    def _fetch_list_member_guids(self, named_account_list_guid: str) -> list:
        """GET .../namedAccountList/{list_guid}/namedAccounts.json and collect all account marketoGUIDs."""

        url = urljoin(self.url_base, f"rest/v1/namedAccountList/{named_account_list_guid}/namedAccounts.json")
        named_account_guids = []
        
        next_page_token = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            params = {}
            if next_page_token:
                params["nextPageToken"] = next_page_token
            prepared = self.build_prepared_request(method="GET", url=url, params=params)
            resp = decorated_request(prepared, None)
            self.validate_response(resp)
            payload = resp.json()
            for item in payload.get("result") or []:
                named_account_guid = item.get("marketoGUID")
                named_account_guids.append(named_account_guid)
            
            
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

        return named_account_guids


    def get_records(self, context):
        named_account_list_guid = (context or {}).get("named_account_list_guid")

        named_account_guids = self._fetch_list_member_guids(named_account_list_guid)
        if not named_account_guids:
            return

        batch_context = (context or {}).copy()
        batch_context["named_account_guids"] = named_account_guids
        yield from super().get_records(batch_context)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any] = None
    ) -> dict:
        params: Dict[str, Any] = {}

        named_account_guids = context and context.get("named_account_guids")
        
        params["filterType"] = "marketoGUID"
        params["filterValues"] = ",".join(named_account_guids)

        props = self.schema.get("properties") or {}
        if props:
            params["fields"] = ",".join(props.keys())

        if next_page_token:
            params["nextPageToken"] = next_page_token
        return params



class ActivityTypeStream(MarketoAsyncRESTStream):
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
                "attributes": {"type": ["null", "object"]},
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
                    "attributes"
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
