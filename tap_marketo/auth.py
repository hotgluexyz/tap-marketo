"""Authentication helpers for Marketo Engage."""

from __future__ import annotations

from urllib.parse import urljoin

import requests

from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from hotglue_singer_sdk.helpers._util import utc_now





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
