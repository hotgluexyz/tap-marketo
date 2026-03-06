"""Tap implementation for Marketo Engage."""

from typing import List
from urllib.parse import urljoin

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_marketo.auth import MarketoAuthenticator
from tap_marketo.streams import (
    ActivityTypesHelperStream,
    ActivityTypeStream,
    LeadsStream
)


BASE_STREAM_TYPES = [
    LeadsStream,
]


class Tapmarketo(Tap):
    """Marketo Engage tap."""

    name = "tap-marketo"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "base_url",
            th.StringType,
            required=True,
            description="Marketo instance base URL, e.g. https://420-ULY-655.mktorest.com",
        ),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="OAuth access token (set by --access-token).",
        ),
        th.Property(
            "expires_in",
            th.IntegerType,
            required=False,
            description="Token expiry in seconds (set by --access-token).",
        ),
        th.Property(
            "last_refreshed",
            th.StringType,
            required=False,
            description="ISO timestamp of last token refresh (set by --access-token).",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest updatedAt timestamp to sync from.",
        ),
    ).to_dict()

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    @classmethod
    def access_token_support(cls, connector=None):
        """Return authenticator class and auth endpoint for token refresh."""
        authenticator = MarketoAuthenticator
        if connector is None:
            base_url = ""  # or a placeholder URL
        else:
            base_url = connector.config.get("base_url", "").rstrip("/") + "/"
        auth_endpoint = urljoin(base_url or "https://example.mktorest.com/", "identity/oauth/token")
        return authenticator, auth_endpoint
        

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in BASE_STREAM_TYPES]

        helper_stream = ActivityTypesHelperStream(tap=self, name="helper")
        activity_types_url = urljoin(
            self.config["base_url"].rstrip("/") + "/",
            "rest/v1/activities/types.json",
        )
        response = helper_stream.make_request(method="GET", url=activity_types_url)
        payload = response.json()

        for activity_type in payload.get("result", []):
            activity_type_name = activity_type.get("name")
            activity_type_id = activity_type.get("id")
            schema = activity_type.get("attributes")
            pk = activity_type.get("primaryAttribute", None)

            if not schema:
                continue
            if activity_type_name and activity_type_id is not None:
                streams.append(
                    ActivityTypeStream(
                        tap=self,
                        activity_type_id=activity_type_id,
                        activity_type_name=activity_type_name,
                        raw_schema=schema,
                        primary_keys=[pk] if pk else [],
                    )
                )

        return streams


if __name__ == "__main__":
    Tapmarketo.cli()
