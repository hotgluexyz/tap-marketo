"""REST client base classes for Marketo streams."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import urljoin
import polars as pl
from hotglue_singer_sdk.streams import AsyncRESTStream
import requests
from hotglue_singer_sdk.typing import AsyncJobStatus
from hotglue_singer_sdk.exceptions import FatalAPIError
from memoization import cached
from tap_marketo.auth import MarketoAuthenticator


class MarketoStream(AsyncRESTStream):
    """Base stream for Marketo REST endpoints."""

    def __init__(self, *args, **kwargs):
        self._http_headers: dict = {}
        self._requests_session = requests.Session()
        super().__init__(*args, **kwargs)

    primary_keys = ["id"]
    replication_key = "updatedAt"

    parallelization_limit = 3

    bulk_export_create_path: str | None = None
    bulk_export_enqueue_path: str | None = None
    bulk_export_status_path: str | None = None
    bulk_export_results_path: str | None = None

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

    def get_paging_windows(self, context: dict | None) -> list[dict[str, Any]]:
        start_date = self.get_starting_time(context, is_inclusive=True)
        end_date = datetime.now(timezone.utc).replace(microsecond=0)


        current_start = start_date.astimezone(timezone.utc).replace(microsecond=0)

        windows: list[dict[str, Any]] = []
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=28), end_date)
            windows.append(
                {
                    "window_start_date": current_start.isoformat().replace("+00:00", "Z"),
                    "window_end_date": current_end.isoformat().replace("+00:00", "Z"),
                }
            )
            current_start = current_end

        return windows

    @property
    @cached
    def authenticator(self) -> MarketoAuthenticator:
        """Return stream authenticator."""
        return MarketoAuthenticator.create_for_stream(self)



    def create_async_job(self, context: dict | None = None) -> dict:

        self.logger.info(f"Creating async job for stream '{self.name}', context: {context}")
        create_path = self.bulk_export_create_path

        create_url = urljoin(self.url_base, create_path)
        create_response = self.make_request(
            method="POST",
            url=create_url,
            context=context,
            json=self.get_async_job_payload(context),
        )
        create_payload = create_response.json()

        result = (create_payload.get("result") or [{}])[0]
        export_id = result.get("exportId")

        enqueue_path_template = self.bulk_export_enqueue_path
        enqueue_path = enqueue_path_template.format(
            job_id=export_id, export_id=export_id, exportId=export_id
        )
        enqueue_url = urljoin(self.url_base, enqueue_path)
        self.make_request(
            method="POST",
            url=enqueue_url,
            context=context,
        )

        return {"job_id": export_id}

    def get_async_job_status(self, job_metadata: dict) -> AsyncJobStatus:
        self.logger.info(f"Getting async job status for stream '{self.name}', job_metadata: {job_metadata}")
        export_id = job_metadata.get("job_id")
        status_path_template = self.bulk_export_status_path

        status_path = status_path_template.format(
            job_id=export_id, export_id=export_id, exportId=export_id
        )
        status_url = urljoin(self.url_base, status_path)
        status_response = self.make_request(method="GET", url=status_url)
        status_payload = status_response.json()

        self.logger.info(f"Status payload: {status_payload}")

        result = (status_payload.get("result") or [{}])[0]
        status_value = str(result.get("status", "")).lower()
        if status_value.lower() == "completed":
            return AsyncJobStatus.COMPLETED
        return AsyncJobStatus.PENDING

    def get_async_job_results(self, job_metadata: Any) -> dict:
        self.logger.info(f"Getting async job results for stream '{self.name}', job_metadata: {job_metadata}")
        export_id = job_metadata.get("job_id")
        results_path_template = self.bulk_export_results_path

        results_path = results_path_template.format(
            job_id=export_id, export_id=export_id, exportId=export_id
        )
        results_url = urljoin(self.url_base, results_path)
        response = self.make_request(method="GET", url=results_url)
        return {
            "job_id": export_id,
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response.text,
        }

    def generate_records_from_job_response(self, job_response: dict) -> Iterable[dict]:
        csv_body = job_response.get("body", "")
        file_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".csv",
                prefix=f"marketo_export_{self.name}_",
                encoding="utf-8",
                delete=False,
            ) as temp_file:
                file_path = temp_file.name
                temp_file.write(csv_body)

            df = pl.read_csv(file_path, infer_schema=False)
            for row in df.to_dicts():
                yield row
        finally:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def post_process(self, row: dict, context) -> dict:
        schema = self.schema
        for key, value in row.items():
            if value == "null":
                row[key] = None
                continue
            if key in schema["properties"]:
                types = schema["properties"][key]["type"]
                is_datetime = schema["properties"][key].get("format") == "date-time"
                if "integer" in types:
                    row[key] = int(value)
                elif "number" in types:
                    row[key] = float(value)
                elif "boolean" in types:
                    row[key] = bool(value)
                elif is_datetime:
                    row[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                elif "string" in types:
                    row[key] = str(value)
                elif "object" in types:
                    row[key] = json.loads(value)
        return row

    def validate_response(self, response: requests.Response) -> None:
        super().validate_response(response)

        try:
            resp_json = response.json()
            if resp_json.get("success") is False:
                errors = resp_json.get("errors") or resp_json
                raise FatalAPIError(f"Marketo API error for stream '{self.name}': {errors}")
        except requests.exceptions.JSONDecodeError:
            pass
