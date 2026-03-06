from hotglue_singer_sdk.tap_base import InvalidCredentialsError

__all__ = ["InvalidCredentialsError", "MissingPermissionsError"]


class MissingPermissionsError(InvalidCredentialsError):
    """Exception raised for missing permission."""
    pass
