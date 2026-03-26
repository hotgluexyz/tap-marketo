"""Authentication helpers for Marketo Engage."""

from __future__ import annotations

from urllib.parse import urljoin


from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


import json
import requests
import re
from singer import utils
import time
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
            token_text = token_response.text or ""
            token_text = re.sub(r'("access_token"\s*:\s*")([^"]*)(".*?"token_type")',
                                lambda m: m.group(1) + (m.group(2)[:8] + ("*" * max(0, len(m.group(2)) - 8))) + m.group(3),
                                token_text, count=1) or token_text
            self.logger.info(f"OAuth authorization attempt was successful, response was '{token_text}'")
        except Exception as ex:
            raise InvalidCredentialsError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = token_json.get("expires_in", self._default_expiration)
        if expires_in is None:
            self.expires_in = None
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it is "
                "expired."
            )
        else:
            self.expires_in = int(expires_in) + int(request_time.timestamp())
        
        self.last_refreshed = request_time
        # Update the tap config with the new access_token
        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["expires_in"] = self.expires_in

        # Write the updated config back to the file (only when config was loaded from a path)
        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        # if expires_in is not set, try to get it from the tap config
        if self.expires_in is None and self._tap.config.get("expires_in"):
            self.expires_in = self._tap.config.get("expires_in")

        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return False

        seconds_left = self.expires_in - int(utils.now().timestamp())
        if seconds_left <= 2:
            if seconds_left >= 0:
                self.logger.info(f"Access token expires in {seconds_left} seconds; waiting so it fully expires before refreshing.")
                time.sleep(seconds_left + 2)            
            return False

        return True
