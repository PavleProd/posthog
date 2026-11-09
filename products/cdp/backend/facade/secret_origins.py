"""
Secret and connection binding for products that store function inputs.

Workflow function steps hold the same inputs as destinations, so workflows checks them with the same
rules: where a saved secret may be sent, whether a save may keep it, and the backfill that binds
existing secrets. The data types live in `facade/contracts.py`.
"""

from products.cdp.backend.management.commands.backfill_secret_origins import Command as BackfillSecretOriginsCommand
from products.cdp.backend.secret_binding import (
    SecretConfig,
    SecretWrite,
    comparable_config,
    comparable_inputs,
    count_origin_outcomes,
    entered_secret_input_keys,
    entry_with_origins,
    input_refusals,
    integration_origin_outcomes,
    secret_binding,
    secret_binding_enforced,
    secret_host_binding_on,
    unchanged_secrets,
)
from products.cdp.backend.secret_origins import (
    UNBOUND,
    SecretDestinations,
    SecretOrigins,
    Unbound,
    mapping_inputs,
    parse_stored_origins,
    public_secret_destinations,
    secret_origins,
    secret_reentry_message,
)
from products.cdp.backend.services.secret_origins_backfill import (
    BackfillPlan,
    BackfillReport,
    BackfillSource,
    SecretOutcome,
    bind_unbound_secrets,
    run_backfill,
)

__all__ = [
    "UNBOUND",
    "BackfillPlan",
    "BackfillReport",
    "BackfillSecretOriginsCommand",
    "BackfillSource",
    "SecretConfig",
    "SecretDestinations",
    "SecretOrigins",
    "SecretOutcome",
    "SecretWrite",
    "Unbound",
    "bind_unbound_secrets",
    "comparable_config",
    "comparable_inputs",
    "count_origin_outcomes",
    "entered_secret_input_keys",
    "entry_with_origins",
    "input_refusals",
    "integration_origin_outcomes",
    "mapping_inputs",
    "parse_stored_origins",
    "public_secret_destinations",
    "run_backfill",
    "secret_binding",
    "secret_binding_enforced",
    "secret_host_binding_on",
    "secret_origins",
    "secret_reentry_message",
    "unchanged_secrets",
]
