"""Authentication helpers for Marketo Engage."""

from __future__ import annotations

import json
from typing import Optional
from urllib.parse import urljoin

import requests
import pendulum

from hotglue_singer_sdk.helpers._util import utc_now
from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from hotglue_singer_sdk.streams import Stream as RESTStreamBase
from tap_marketo.exceptions import InvalidCredentialsError



class MarketoAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """OAuth authenticator using Marketo's identity endpoint."""

    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None
    ) -> None:
        super().__init__(stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes)
        self._tap = stream._tap

    @property
    def oauth_request_payload(self) -> dict:
        """Query params for token request."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.expires_in is not None:
            self.expires_in = int(self.expires_in)
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True

        last_refreshed_dt = self.last_refreshed if isinstance(self.last_refreshed, pendulum.DateTime) else pendulum.parse(self.last_refreshed) # if using pendulum
        if self.expires_in > (utc_now() - last_refreshed_dt).total_seconds():
            return True
        return False

    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            InvalidCredentialsError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise InvalidCredentialsError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            ) from ex
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", None)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time

        self._tap._config["access_token"] = token_json["access_token"]

        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)

    @classmethod
    def create_for_stream(cls, stream) -> "MarketoAuthenticator":
        """Create a stream authenticator from tap config."""
        base_url = stream.config["base_url"].rstrip("/") + "/"
        return cls(
            stream=stream,
            auth_endpoint=urljoin(base_url, "identity/oauth/token"),
        )
