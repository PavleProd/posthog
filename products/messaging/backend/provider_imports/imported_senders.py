"""Fills in the sender of a template imported from another email tool."""

from collections.abc import Mapping
from typing import Any

from posthog.integrations import IntegrationRecord


def apply_sender(
    email: dict[str, Any], display_name: str, address: str, senders: Mapping[str, IntegrationRecord]
) -> tuple[str, ...]:
    """Sets `email["from"]` when `address` is a PostHog sender, and returns notes on what the person still has to do."""
    sender = senders.get(address.lower())
    if sender is None:
        return (f"{address} isn't a PostHog sender yet. Add it, then choose it on this template.",)
    email["from"] = {
        "integrationId": sender.id,
        "email": address,
        "name": display_name or str(sender.config.get("name") or ""),
    }
    if not sender.config.get("verified"):
        return (f"{address} isn't verified in PostHog yet, so this template can't send until it is.",)
    return ()
