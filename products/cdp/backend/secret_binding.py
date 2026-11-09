"""Whether a save may keep a stored secret, or a connection, where its config now sends it. Shared by
every API that stores function inputs: destinations and workflow function steps."""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import field
from enum import StrEnum
from functools import cache
from typing import Any, Optional

import posthoganalytics
from prometheus_client import Counter
from rest_framework.exceptions import ErrorDetail

from posthog.dataclasses import frozen
from posthog.integrations import IntegrationKind, IntegrationManager

from products.cdp.backend.integration_origins import (
    integration_inputs,
    integration_origins,
    integration_refusal_message,
    integration_worker_calls,
    public_config,
)
from products.cdp.backend.secret_origins import SecretOrigins, mapping_inputs, secret_origins, stored_origins
from products.cdp.backend.validation import MASKED_SECRET_VALUE

SECRET_HOST_BINDING_FLAG = "cdp-secret-host-binding"
SECRET_REENTRY_CODE = "secret_reentry_required"
INTEGRATION_REFUSAL_CODE = "integration_origin_refused"
SECRET_HOST_BINDING_COUNTER = Counter(
    "posthog_cdp_secret_host_binding_total",
    "Secret and integration inputs checked against the origins they may be sent to, by outcome, by the API "
    "surface that saved them, and by integration kind.",
    labelnames=["outcome", "surface", "kind"],
)


class SecretBinding(StrEnum):
    BOUND = "bound"
    KEPT = "kept"
    REFUSED = "refused"
    # An integration whose code sends its credentials only to the origins of its kind.
    WITHIN = "within"


class SecretBindingMetric(StrEnum):
    BOUND = "bound"
    KEPT = "kept"
    REFUSED = "refused"
    WITHIN = "within"
    WOULD_REFUSE = "would_refuse"
    STAGED = "staged"


def binding_metric(outcome: SecretBinding, let_through: bool, stages_draft: bool) -> SecretBindingMetric:
    if not let_through:
        return SecretBindingMetric(outcome.value)
    return SecretBindingMetric.STAGED if stages_draft else SecretBindingMetric.WOULD_REFUSE


class OriginCheck(StrEnum):
    SECRET = "secret"
    INTEGRATION = "integration"
    INTEGRATION_UNLISTED = "integration_unlisted"


# A key refused by several checks shows the error of the first check here. Entering the secret again
# does not fix an integration refusal, so those go first and the user does not retype for nothing.
REFUSAL_CODES: dict[OriginCheck, str] = {
    OriginCheck.INTEGRATION: INTEGRATION_REFUSAL_CODE,
    OriginCheck.INTEGRATION_UNLISTED: INTEGRATION_REFUSAL_CODE,
    OriginCheck.SECRET: SECRET_REENTRY_CODE,
}


@frozen
class OriginOutcome:
    key: str
    check: OriginCheck
    binding: SecretBinding
    # The integration kind, or "" for a secret. It labels the metric, so only a known kind is kept.
    kind: str = ""
    # What a refusal tells the user, set only when the binding is refused.
    message: str = ""

    def metric_outcome(self, enforced: bool, stages_draft: bool) -> str:
        let_through = self.binding == SecretBinding.REFUSED and not enforced
        metric = binding_metric(self.binding, let_through, stages_draft)
        return metric if self.check == OriginCheck.SECRET else f"{self.check}_{metric}"


def count_origin_outcomes(outcomes: Iterable[OriginOutcome], surface: str, enforced: bool, stages_draft: bool) -> None:
    for item in outcomes:
        SECRET_HOST_BINDING_COUNTER.labels(
            outcome=item.metric_outcome(enforced, stages_draft), surface=surface, kind=item.kind
        ).inc()


def refusal_errors(outcomes: Iterable[OriginOutcome]) -> dict[str, ErrorDetail]:
    """The error for each refused key, from the first check in `REFUSAL_CODES` that refused it."""
    outcomes = list(outcomes)
    errors: dict[str, ErrorDetail] = {}
    for check, code in REFUSAL_CODES.items():
        for item in outcomes:
            if item.check == check and item.binding == SecretBinding.REFUSED:
                errors.setdefault(item.key, ErrorDetail(item.message, code=code))
    return errors


def comparable_inputs(items: Any) -> dict:
    return {
        key: (item.get("value"), item.get("templating") or "hog")
        for key, item in (items or {}).items()
        if isinstance(item, dict)
    }


# A schema change can turn an input into a secret or an integration, so it changes what the code receives.
def _comparable_schema(schemas: Any) -> list[str]:
    return sorted(
        repr((schema.get("key"), schema.get("type"), bool(schema.get("secret")), schema.get("integration")))
        for schema in schemas or []
        if isinstance(schema, dict)
    )


@frozen
class SecretConfig:
    """What decides where a config sends its secrets."""

    code: str
    inputs: dict[str, Any] = field(repr=False)
    mappings: Any = field(repr=False)
    inputs_schema: Any = None
    # Anything else the code reads that can change an address, such as workflow variables. Compared
    # for equality only.
    context: Any = None


def comparable_config(config: SecretConfig, secret_keys: set[str]) -> tuple:
    public_inputs = {key: item for key, item in config.inputs.items() if key not in secret_keys}
    mapping_dicts = [mapping for mapping in config.mappings or [] if isinstance(mapping, dict)]
    return (
        config.code,
        comparable_inputs(public_inputs),
        _comparable_schema(config.inputs_schema),
        [(comparable_inputs(m.get("inputs")), _comparable_schema(m.get("inputs_schema"))) for m in mapping_dicts],
        config.context,
    )


def unchanged_secrets(
    config: SecretConfig, stored: SecretConfig, secret_keys: set[str], derived: Mapping[str, SecretOrigins]
) -> set[str]:
    """The secrets in `derived` that `config` sends where `stored` sends them. `secret_keys` names every
    secret input of the config. A secret's own value is compared because a dictionary secret can hold
    the address it is sent to."""
    if comparable_config(config, secret_keys) != comparable_config(stored, secret_keys):
        return set()
    moved = {
        key
        for key in secret_keys
        if comparable_inputs({key: config.inputs.get(key)}) != comparable_inputs({key: stored.inputs.get(key)})
    }
    kept = set(derived) - moved
    if not moved or not kept:
        return kept
    with_stored = {key: item for key, item in config.inputs.items() if key not in moved}
    with_stored.update({key: stored.inputs[key] for key in moved if key in stored.inputs})
    restored = secret_origins(config.code, with_stored, mapping_inputs(config.mappings), kept)
    return {key for key in kept if restored[key] == derived[key]}


def entered_secret_input_keys(raw_inputs: Any) -> set[str]:
    """Input keys whose request value replaces the stored secret. A dictionary secret sent with some
    entries still masked keeps those stored entries, so it does not count as entered."""
    if not isinstance(raw_inputs, dict):
        return set()
    return {
        key
        for key, item in raw_inputs.items()
        if isinstance(item, dict)
        and "value" in item
        and item["value"] != MASKED_SECRET_VALUE
        and not (isinstance(item["value"], dict) and MASKED_SECRET_VALUE in item["value"].values())
    }


def secret_binding(
    derived: SecretOrigins, baseline: Optional[SecretOrigins], entered: bool, unchanged: bool
) -> tuple[SecretOrigins, SecretBinding]:
    """The origins a secret may be sent to after a save, and whether the save binds, keeps or is
    refused. A secret with no baseline is new, so it binds like an entered one."""
    if entered or baseline is None:
        return derived, SecretBinding.BOUND
    sends_within = isinstance(derived, frozenset) and isinstance(baseline, frozenset) and derived <= baseline
    return baseline, SecretBinding.KEPT if unchanged or sends_within else SecretBinding.REFUSED


class SecretWrite(StrEnum):
    """What a save stores for a refused secret it lets through."""

    # Where the code now sends it.
    LIVE = "live"
    # Its stored origins, for a draft whose publish compares the whole config with the live one.
    STAGED_KEEPING_ORIGINS = "staged_keeping_origins"
    # No origins, so that publishing checks the secret against the live config again.
    STAGED_UNBOUND = "staged_unbound"


def entry_with_origins(
    entry: dict[str, Any], origins: SecretOrigins, outcome: SecretBinding, derived: SecretOrigins, write: SecretWrite
) -> dict[str, Any]:
    updated = {key: value for key, value in entry.items() if key != "allowed_origins"}
    if outcome != SecretBinding.REFUSED or write == SecretWrite.STAGED_KEEPING_ORIGINS:
        updated["allowed_origins"] = stored_origins(origins)
    elif write == SecretWrite.LIVE:
        updated["allowed_origins"] = stored_origins(derived)
    return updated


def secret_binding_enforced(context: Mapping[str, Any]) -> bool:
    """Whether a refused secret fails the save that `context` validates. Only a request fails: a caller
    with no view, such as a management command, re-saves what someone already stored. A request fails
    only while the flag is on for its team."""
    if context.get("view") is None:
        return False
    team = context["get_team"]()
    return bool(
        posthoganalytics.feature_enabled(
            SECRET_HOST_BINDING_FLAG,
            str(team.uuid),
            groups={"organization": str(team.organization_id)},
            send_feature_flag_events=False,
        )
    )


def integration_origin_outcomes(
    team_id: int, config: SecretConfig, unchanged: Callable[[], bool]
) -> list[OriginOutcome]:
    """One outcome per connection of each integration input of `config`. A connection whose credentials
    go only to its kind's origins is within. Any other is kept when `unchanged` says the config is the
    stored one, and refused otherwise. `unchanged` is called only when some connection needs it."""
    ids_by_key: dict[str, set[int]] = {}
    multi_keys: set[str] = set()
    for item in integration_inputs(config.inputs_schema, config.inputs, config.mappings):
        ids_by_key.setdefault(item.key, set()).update(item.integration_ids)
        if item.holds_list:
            multi_keys.add(item.key)
    rows = IntegrationManager.any.get_many(team_id, set().union(*ids_by_key.values())) if ids_by_key else {}
    connections = {key: [rows[i] for i in ids if i in rows] for key, ids in ids_by_key.items()}
    connections = {key: found for key, found in connections.items() if found}
    if not connections:
        return []

    configs = {
        key: public_config(found[0].config, found[0].sensitive_config)
        for key, found in connections.items()
        if len(found) == 1 and key not in multi_keys
    }
    # A key may be handed to a worker function only when every connection it holds allows it.
    worker_calls = {
        key: frozenset.intersection(*(integration_worker_calls(row.kind) for row in found))
        for key, found in connections.items()
    }
    derived = secret_origins(
        config.code, config.inputs, mapping_inputs(config.mappings), set(connections), configs, worker_calls
    )

    config_unchanged = cache(unchanged)
    outcomes = []
    for key, found in connections.items():
        origins = derived[key]
        for row in found:
            allowed = integration_origins(row.kind, row.config)
            # An unlisted kind has no origins to check against, so like an unbound secret it only keeps
            # a config it already had.
            kind_check = OriginCheck.INTEGRATION if allowed is not None else OriginCheck.INTEGRATION_UNLISTED
            if origins == frozenset() or (
                allowed is not None and isinstance(origins, frozenset) and origins <= allowed
            ):
                binding = SecretBinding.WITHIN
            else:
                binding = SecretBinding.KEPT if config_unchanged() else SecretBinding.REFUSED
            kind = row.kind if row.kind in IntegrationKind else "other"
            message = integration_refusal_message(row.kind, row.config)
            outcomes.append(OriginOutcome(key=key, check=kind_check, binding=binding, kind=kind, message=message))
    return outcomes
