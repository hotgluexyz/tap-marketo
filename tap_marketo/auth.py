"""Authentication helpers for Marketo Engage."""

from __future__ import annotations

from urllib.parse import urljoin


from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


import json
import requests
from hotglue_singer_sdk.helpers._util import utc_now
from hotglue_etl_exceptions import InvalidCredentialsError


class MarketoAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """OAuth authenticator using Marketo's identity endpoint."""

    @property
    def oauth_request_payload(self) -> dict:
        """Query params for token request."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }



    @classmethod
    def create_for_stream(cls, stream) -> "MarketoAuthenticator":
        """Create a stream authenticator from tap config."""
        base_url = stream.config["base_url"].rstrip("/") + "/"
        return cls(
            stream=stream,
            auth_endpoint=urljoin(base_url, "identity/oauth/token"),
        )
    
    def update_access_token_locally(self) -> None:
        """Update `access_token` locally."""

        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload, auth=self.request_auth())
        try:
            token_response.raise_for_status()
            self.logger.info(f"OAuth authorization attempt was successful, response was '{token_response.text}")
        except Exception as ex:
            raise InvalidCredentialsError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = int(token_json.get("expires_in", self._default_expiration))
        self.expires_in = expires_in + int(request_time.timestamp())
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time
        # Update the tap config with the new access_token and refresh_token
        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["expires_in"] = self.expires_in
        if token_json.get("refresh_token"):
            # Log the refresh_token
            self._tap.logger.info(f"Latest refresh token: {token_json.get('refresh_token')}")
            self._tap._config["refresh_token"] = token_json["refresh_token"]

        # Write the updated config back to the file (only when config was loaded from a path)
        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
