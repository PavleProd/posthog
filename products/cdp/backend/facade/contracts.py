"""Data types other products read from cdp."""

from enum import StrEnum

from posthog.dataclasses import frozen


class SecretBinding(StrEnum):
    BOUND = "bound"
    KEPT = "kept"
    REFUSED = "refused"
    # An integration whose code sends its credentials only to the origins of its kind.
    WITHIN = "within"


class OriginCheck(StrEnum):
    SECRET = "secret"
    INTEGRATION = "integration"
    INTEGRATION_UNLISTED = "integration_unlisted"


@frozen
class InputRefusal:
    """Why a save may not keep an input, and the error code the API reports it with."""

    message: str
    code: str


@frozen
class OriginOutcome:
    key: str
    check: OriginCheck
    binding: SecretBinding
    # The integration kind, or "" for a secret. It labels the metric, so only a known kind is kept.
    kind: str = ""
    # What a refusal tells the user, set only when the binding is refused.
    message: str = ""
