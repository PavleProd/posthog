"""Secret function-action inputs: split out of a workflow's actions for encryption, masked on read,
and folded back in for inline test runs, and each one bound, with each connection, to the origins its
step may send it to."""

from products.cdp.backend.secret_binding import (
    SecretBinding,
    count_origin_outcomes,
    refusal_errors,
    secret_binding_enforced,
)
from products.workflows.backend.facade.contracts import FlowSecretBinding
from products.workflows.backend.services.hog_flow_secret_origins import (
    bind_flow_secret_origins,
    entered_secret_keys,
    sends_steered_by_supplied_variables,
)
from products.workflows.backend.services.hog_flow_secret_origins_backfill import backfill_hog_flow_secret_origins
from products.workflows.backend.services.hog_flow_secrets import (
    TemplateCache,
    mask_derived_trigger,
    mask_secret_action_inputs,
    mask_workflow_fields,
    merge_secret_maps,
    partition_flow_secrets,
    plaintext_secret_map,
    recover_or_drop_masked_inputs,
    rehydrate_flow_secrets,
    secret_keys_for_action,
    strip_content_secrets,
    strip_secrets_from_content,
)

__all__ = [
    "SecretBinding",
    "count_origin_outcomes",
    "refusal_errors",
    "secret_binding_enforced",
    "FlowSecretBinding",
    "TemplateCache",
    "backfill_hog_flow_secret_origins",
    "bind_flow_secret_origins",
    "entered_secret_keys",
    "mask_derived_trigger",
    "mask_secret_action_inputs",
    "mask_workflow_fields",
    "merge_secret_maps",
    "partition_flow_secrets",
    "plaintext_secret_map",
    "recover_or_drop_masked_inputs",
    "rehydrate_flow_secrets",
    "secret_keys_for_action",
    "sends_steered_by_supplied_variables",
    "strip_content_secrets",
    "strip_secrets_from_content",
]
