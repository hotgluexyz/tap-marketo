"""Basic tests for tap-marketo."""

from tap_marketo.tap import Tapmarketo


def test_tap_name():
    """Tap name should be stable for singer discovery."""
    tap = Tapmarketo(config={"base_url": "https://example.mktorest.com", "client_id": "x", "client_secret": "y"})
    assert tap.name == "tap-marketo"
