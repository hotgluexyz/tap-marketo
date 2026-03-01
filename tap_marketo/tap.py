"""Tap implementation for Marketo Engage."""

from typing import List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th

from tap_marketo.streams import (
    LeadsStream,
)

BASE_STREAM_TYPES = [
    LeadsStream,
]

class Tapmarketo(Tap):
    """Marketo Engage tap."""

    name = "tap-marketo"

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
            "start_date",
            th.DateTimeType,
            description="Earliest updatedAt timestamp to sync from.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in BASE_STREAM_TYPES]



if __name__ == "__main__":
    Tapmarketo.cli()
