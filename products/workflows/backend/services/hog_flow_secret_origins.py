"""Bind each secret input of a workflow step to the origins the step's template code can send it to, and
check that each connection a step holds sends its credentials only to its kind's origins, with the rules
in `products.cdp.backend.secret_binding`.

A save restores a stored secret by step id and key, the staged draft's copy first, so each step is
compared with the step on the side its secret came from. A staged secret without origins was never
checked against its draft step, so it is checked against the live one.

A step whose address reads a workflow variable sends its secret wherever that variable points when the
step runs: the variable's default, or what an earlier step wrote to it. So a step that reads variables
is compared together with its variable defaults and, when an earlier step can write one of them, with
every step that runs before it and the edges between them.
"""

import re
import json
import dataclasses
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from posthog.dataclasses import frozen

from products.cdp.backend.secret_binding import (
    OriginCheck,
    OriginOutcome,
    SecretBinding,
    SecretConfig,
    SecretWrite,
    comparable_config,
    comparable_inputs,
    entered_secret_input_keys,
    entry_with_origins,
    integration_origin_outcomes,
    secret_binding,
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
from products.workflows.backend.facade.contracts import FlowSecretBinding, WorkflowEditState
from products.workflows.backend.services.hog_flow_secrets import (
    TemplateCache,
    function_template_for_action,
    merge_secret_maps,
    plaintext_secret_map,
    secret_keys_for_action,
)

# Fields of a step that change only how the editor shows it.
_DISPLAY_FIELDS = frozenset({"name", "description", "position", "created_at", "updated_at"})

# A reference to a workflow variable by name. Liquid names may hold hyphens, which Hog reads as a
# subtraction, so both readings count. Any other use of `variables`, such as an index by a computed
# key, can read every variable.
_VARIABLE_REFERENCE = re.compile(r"\bvariables\b(?:\s*\.\s*(\w[\w-]*))?")

_NOWHERE = SecretDestinations(hosts=frozenset(), built_from=frozenset())


def _template_code(action: dict, template_cache: TemplateCache) -> str:
    # A step whose template is gone never runs, so it sends nothing.
    template = function_template_for_action(action, template_cache)
    return (template.code or "") if template else ""


def _template_schema(action: dict, template_cache: TemplateCache) -> list[dict]:
    template = function_template_for_action(action, template_cache)
    return [schema for schema in (template.inputs_schema or []) if isinstance(schema, dict)] if template else []


def _text(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _steps(actions: Any) -> dict[str, dict]:
    return {
        str(action["id"]): action
        for action in (actions if isinstance(actions, list) else [])
        if isinstance(action, dict) and action.get("id")
    }


def _secret_map(secrets: Any) -> dict[str, Any]:
    return secrets if isinstance(secrets, dict) else {}


def _draft(stored: WorkflowEditState) -> dict:
    return stored.draft if isinstance(stored.draft, dict) else {}


def _entry(secrets: dict[str, Any], step_id: str, key: str) -> Optional[dict]:
    step_secrets = secrets.get(step_id)
    entry = step_secrets.get(key) if isinstance(step_secrets, dict) else None
    return entry if isinstance(entry, dict) else None


def _referenced_variables(texts: Iterable[str]) -> Optional[set[str]]:
    """The names of the variables `texts` read, or None when they may read any."""
    names: set[str] = set()
    for text in texts:
        for match in _VARIABLE_REFERENCE.finditer(text):
            name = match.group(1)
            if name is None:
                return None
            names.update({name, name.split("-")[0]})
    return names


def _writes_any(action: dict, names: Optional[set[str]]) -> bool:
    output_variable = action.get("output_variable")
    entries = output_variable if isinstance(output_variable, list) else [output_variable]
    for entry in entries:
        key = entry.get("key") if isinstance(entry, dict) else None
        if not key:
            continue
        # A spread output writes one variable per property of the result, each named `<key>_<property>`.
        spread = isinstance(entry, dict) and entry.get("spread")
        if names is None or key in names or (spread and any(n.startswith(f"{key}_") for n in names)):
            return True
    return False


def _variable_defaults(variables: Any, names: Optional[set[str]]) -> dict[Any, Optional[str]]:
    # The API stores every default as text, so a re-save compares equal to a row written another way.
    return {
        item.get("key"): _text(item.get("default"))
        for item in variables or []
        if isinstance(item, dict) and (names is None or item.get("key") in names)
    }


def _without_bytecode(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _without_bytecode(item) for key, item in value.items() if key != "bytecode"}
    if isinstance(value, list):
        return [_without_bytecode(item) for item in value]
    return value


def _edge_endpoints(edge: Any) -> tuple[Optional[str], Optional[str]]:
    if not isinstance(edge, dict):
        return None, None
    return _text(edge.get("from")), _text(edge.get("to"))


class _Flow:
    """One side of a workflow, as the worker would run it, with what each step's run depends on."""

    def __init__(
        self,
        actions: dict[str, dict],
        edges: Any,
        variables: Any,
        secrets: dict[str, dict],
        template_cache: TemplateCache,
    ) -> None:
        self.actions = actions
        self.variables = variables
        # The secrets the side's steps run with, by step id and key. A draft runs with the live secrets
        # it does not stage. Steps that carry their secrets restored need none here.
        self.secrets = secrets
        self._template_cache = template_cache
        self.edges = [edge for edge in (edges if isinstance(edges, list) else []) if isinstance(edge, dict)]
        self._parents: dict[str, set[str]] = {}
        for edge in self.edges:
            source, target = _edge_endpoints(edge)
            if source and target:
                self._parents.setdefault(target, set()).add(source)
        self._reads: dict[str, Optional[set[str]]] = {}
        self._run_texts: dict[str, str] = {}
        self._contexts: dict[str, tuple] = {}

    def inputs(self, step_id: str) -> dict[str, Any]:
        action = self.actions.get(step_id) or {}
        return {**((action.get("config") or {}).get("inputs") or {}), **(self.secrets.get(step_id) or {})}

    def _read_variables(self, step_id: str) -> Optional[set[str]]:
        if step_id not in self._reads:
            action = self.actions[step_id]
            # A secret input can hold its own address, so its text counts as a read too.
            texts = [_template_code(action, self._template_cache), self._run_text(step_id)]
            self._reads[step_id] = _referenced_variables(texts)
        return self._reads[step_id]

    def _run_text(self, step_id: str) -> str:
        """Everything about a step that changes what its run does, secrets included."""
        if step_id not in self._run_texts:
            action = self.actions[step_id]
            config = action.get("config") or {}
            self._run_texts[step_id] = json.dumps(
                {
                    **_without_bytecode({k: v for k, v in action.items() if k not in _DISPLAY_FIELDS | {"config"}}),
                    "config": {
                        **_without_bytecode({k: v for k, v in config.items() if k not in {"inputs", "mappings"}}),
                        "inputs": comparable_inputs(self.inputs(step_id)),
                        "mappings": [comparable_inputs(items) for items in mapping_inputs(config.get("mappings"))],
                    },
                },
                sort_keys=True,
                default=str,
            )
        return self._run_texts[step_id]

    def _runs_before(self, step_id: str) -> set[str]:
        seen: set[str] = set()
        pending = [step_id]
        while pending:
            for parent in self._parents.get(pending.pop(), ()):
                if parent not in seen and parent != step_id:
                    seen.add(parent)
                    pending.append(parent)
        return seen

    def context(self, step_id: str) -> tuple:
        """What, besides the step itself, decides where the step's run sends its secrets."""
        if step_id not in self._contexts:
            names = self._read_variables(step_id)
            if names is not None and not names:
                self._contexts[step_id] = ()
            else:
                before = {other for other in self._runs_before(step_id) if other in self.actions}
                if not any(_writes_any(self.actions[other], names) for other in before):
                    self._contexts[step_id] = (_variable_defaults(self.variables, names),)
                else:
                    # An earlier step can write a variable the step reads, and which steps run, and in what
                    # order, decides its value. So every earlier step counts, and every default it can read.
                    path = before | {step_id}
                    self._contexts[step_id] = (
                        _variable_defaults(self.variables, None),
                        sorted((other, self._run_text(other)) for other in before),
                        sorted(
                            json.dumps(edge, sort_keys=True, default=str)
                            for edge in self.edges
                            if set(_edge_endpoints(edge)) <= path
                        ),
                    )
        return self._contexts[step_id]

    def reads_any(self, step_id: str, names: set[str]) -> bool:
        """Whether a run of the step can depend on one of the variables `names`. Once an earlier step can
        write a variable the step reads, any earlier step that reads one can change what the step gets."""
        read = self._read_variables(step_id)
        if read is None or read & names:
            return True
        before = {other for other in self._runs_before(step_id) if other in self.actions}
        if not any(_writes_any(self.actions[other], read) for other in before):
            return False
        return any((other_read := self._read_variables(other)) is None or other_read & names for other in before)

    def secret_config(self, step_id: str, inputs: Optional[dict] = None) -> SecretConfig:
        action = self.actions[step_id]
        return SecretConfig(
            code=_template_code(action, self._template_cache),
            inputs=self.inputs(step_id) if inputs is None else inputs,
            mappings=(action.get("config") or {}).get("mappings"),
            inputs_schema=_template_schema(action, self._template_cache),
            context=self.context(step_id),
        )


@frozen
class _Store:
    secrets: dict[str, Any] = dataclasses.field(repr=False)
    side: _Flow


@frozen
class _Recovered:
    side: _Flow
    entry: Optional[dict] = dataclasses.field(repr=False)


class _StoredFlows:
    def __init__(self, stored: WorkflowEditState, live_secrets_only: bool, template_cache: TemplateCache) -> None:
        draft = _draft(stored)
        draft_actions = draft.get("actions", stored.actions)
        live_plaintext = plaintext_secret_map(stored.actions, template_cache)
        draft_plaintext = plaintext_secret_map(draft_actions, template_cache)
        live_secrets = merge_secret_maps(live_plaintext, _secret_map(stored.encrypted_inputs))
        self.live = _Flow(_steps(stored.actions), stored.edges, stored.variables, live_secrets, template_cache)
        self.draft = _Flow(
            _steps(draft_actions),
            draft.get("edges", stored.edges),
            draft.get("variables", stored.variables),
            merge_secret_maps(
                merge_secret_maps(merge_secret_maps(live_plaintext, draft_plaintext), live_secrets),
                _secret_map(stored.draft_encrypted_inputs),
            ),
            template_cache,
        )
        self.has_draft = bool(draft)
        # The order a save recovers a secret in, first match winning, as `existing_secret_map` merges them.
        stores = [
            _Store(secrets=_secret_map(stored.draft_encrypted_inputs), side=self.draft),
            _Store(secrets=_secret_map(stored.encrypted_inputs), side=self.live),
            _Store(secrets=draft_plaintext, side=self.draft),
            _Store(secrets=live_plaintext, side=self.live),
        ]
        self.stores = [store for store in stores if not (store.side is self.draft and live_secrets_only)]

    def recover(self, step_id: str, key: str) -> Optional[_Recovered]:
        for store in self.stores:
            entry = _entry(store.secrets, step_id, key)
            if entry is None:
                continue
            if store.side is self.draft and parse_stored_origins(entry.get("allowed_origins")) is None:
                return _Recovered(side=self.live, entry=_entry(self.live.secrets, step_id, key))
            return _Recovered(side=store.side, entry=entry)
        return None


def _unchanged(config: SecretConfig, step_id: str, key: str, derived: SecretOrigins, side: _Flow) -> bool:
    if step_id not in side.actions:
        return False
    stored = side.secret_config(step_id)
    secret_keys = {schema["key"] for schema in stored.inputs_schema if schema.get("secret") and "key" in schema}
    return key in unchanged_secrets(config, stored, secret_keys, {key: derived})


def _baseline(key: str, step_id: str, recovered: _Recovered, unchanged: bool) -> Optional[SecretOrigins]:
    """The origins the stored secret may go to, or None when it was saved before origins were bound
    and its step is unchanged."""
    entry = recovered.entry
    baseline = parse_stored_origins(entry.get("allowed_origins")) if entry is not None else None
    if baseline is not None or unchanged:
        return baseline
    if step_id not in recovered.side.actions:
        return UNBOUND
    stored = recovered.side.secret_config(step_id)
    return secret_origins(stored.code, stored.inputs, mapping_inputs(stored.mappings), {key})[key]


def _labels(schemas: list[dict]) -> dict[str, str]:
    return {schema["key"]: schema.get("label") or schema["key"] for schema in schemas if "key" in schema}


def _public_destinations(config: SecretConfig, keys: set[str]) -> dict[str, SecretDestinations | Unbound]:
    secret_keys = keys | {schema["key"] for schema in config.inputs_schema if schema.get("secret") and "key" in schema}
    return public_secret_destinations(config.code, config.inputs, mapping_inputs(config.mappings), secret_keys)


def _reentry_message(key: str, config: SecretConfig, keys: set[str], before_side: Optional[_Flow], step_id: str) -> str:
    """Why a step's secret has to be entered again. The editor who reads it may not read the secrets, so
    both what the step sends now and what it sent before come from an analysis that holds no secret
    values."""
    now = _public_destinations(config, keys)[key]
    before: SecretDestinations | Unbound = _NOWHERE
    if before_side is not None and step_id in before_side.actions:
        before = _public_destinations(before_side.secret_config(step_id), {key}).get(key, _NOWHERE)
    return secret_reentry_message(key, now, before, _labels(config.inputs_schema))


def entered_secret_keys(sent_actions: Any, written_actions: Any = None) -> dict[str, set[str]]:
    """Secret input keys by step id whose new value the request carries. Pass `written_actions` when the
    request patches stored steps: a patch merges into the stored inputs, which on a row saved before
    encryption still hold secrets in plaintext, so a key counts only when the request sets its whole
    written value."""
    entered = {
        str(action.get("id")): entered_secret_input_keys(action["config"].get("inputs"))
        for action in (sent_actions if isinstance(sent_actions, list) else [])
        if isinstance(action, dict) and isinstance(action.get("config"), dict)
    }
    if written_actions is None:
        return entered
    sent, written = _steps(sent_actions), _steps(written_actions)

    def value(steps: dict[str, dict], step_id: str, key: str) -> Any:
        item = (((steps.get(step_id) or {}).get("config") or {}).get("inputs") or {}).get(key)
        return item.get("value") if isinstance(item, dict) else None

    return {
        step_id: {key for key in keys if value(written, step_id, key) == value(sent, step_id, key)}
        for step_id, keys in entered.items()
    }


def _secret_keys(action: dict, template_cache: TemplateCache) -> set[str]:
    inputs = (action.get("config") or {}).get("inputs")
    if not isinstance(inputs, dict):
        return set()
    return {key for key in secret_keys_for_action(action, template_cache) if isinstance(inputs.get(key), dict)}


def _function_steps(flow: _Flow) -> list[str]:
    return [
        step_id
        for step_id, action in flow.actions.items()
        if isinstance((action.get("config") or {}).get("inputs"), dict)
    ]


def _connection_kept_unchanged(flow: _Flow, step_id: str, stored_sides: list[_Flow]) -> bool:
    config = comparable_config(flow.secret_config(step_id), set())
    return any(
        step_id in side.actions and comparable_config(side.secret_config(step_id), set()) == config
        for side in stored_sides
    )


def bind_flow_secret_origins(
    actions: list[dict],
    *,
    team_id: int,
    stored: Optional[WorkflowEditState],
    edges: Any,
    variables: Any,
    entered: dict[str, set[str]],
    live_secrets_only: bool,
    writes_draft: bool,
    template_cache: TemplateCache,
) -> FlowSecretBinding:
    """Set `allowed_origins` on every secret input of `actions`, which carry their secrets restored, and
    check every connection they hold. `edges` and `variables` are None when the save keeps the stored
    ones. `writes_draft` is true only when the save stages a draft: publish and a test run of the draft
    send what they check, like a live save."""
    stored_flows = _StoredFlows(stored, live_secrets_only, template_cache) if stored is not None else None
    if stored_flows is not None:
        kept = stored_flows.draft if writes_draft else stored_flows.live
        edges = kept.edges if edges is None else edges
        variables = kept.variables if variables is None else variables
    flow = _Flow(_steps(actions), edges, variables, {}, template_cache)
    # A connection may keep a config that some stored side already ran with. Publish compares with the
    # live side only, because the draft it promotes may never have been checked.
    connection_sides = []
    if stored_flows is not None:
        connection_sides = [stored_flows.live]
        if writes_draft and stored_flows.has_draft:
            connection_sides.append(stored_flows.draft)
    write = SecretWrite.STAGED_UNBOUND if writes_draft else SecretWrite.LIVE

    outcomes: dict[str, list[OriginOutcome]] = {}
    rewrites_secrets = False
    for step_id in _function_steps(flow):
        action = flow.actions[step_id]
        keys = _secret_keys(action, template_cache)
        step_outcomes: list[OriginOutcome] = []
        if keys:
            inputs = action["config"]["inputs"]
            config = flow.secret_config(step_id)
            derived = secret_origins(config.code, inputs, mapping_inputs(config.mappings), keys)
            for key in sorted(keys):
                recovered = stored_flows.recover(step_id, key) if stored_flows is not None else None
                is_entered = key in entered.get(step_id, set())
                baseline: Optional[SecretOrigins] = None
                unchanged = False
                if recovered is not None and not is_entered:
                    unchanged = _unchanged(config, step_id, key, derived[key], recovered.side)
                    baseline = _baseline(key, step_id, recovered, unchanged)
                    baseline = derived[key] if baseline is None else baseline
                origins, binding = secret_binding(derived[key], baseline, is_entered, unchanged)
                entry = entry_with_origins(inputs[key], origins, binding, derived[key], write)
                rewrites_secrets = rewrites_secrets or entry != inputs[key]
                inputs[key] = entry
                message = ""
                if binding == SecretBinding.REFUSED:
                    message = _reentry_message(key, config, keys, recovered.side if recovered else None, step_id)
                step_outcomes.append(OriginOutcome(key=key, check=OriginCheck.SECRET, binding=binding, message=message))
        step_outcomes += integration_origin_outcomes(
            team_id,
            flow.secret_config(step_id),
            lambda step_id=step_id: _connection_kept_unchanged(flow, step_id, connection_sides),
        )
        if step_outcomes:
            outcomes[step_id] = step_outcomes
    return FlowSecretBinding(outcomes=outcomes, rewrites_secrets=rewrites_secrets)


def sends_steered_by_supplied_variables(
    actions: list[dict],
    *,
    team_id: int,
    edges: Any,
    supplied: Mapping[str, Any],
    variables: Any,
    entered: dict[str, set[str]],
    template_cache: TemplateCache,
) -> FlowSecretBinding:
    """The secrets and connections that a run supplying `supplied` as variable values could send to an
    address no check has seen, each refused. Only a secret that may go to any origin, or a connection
    whose code may send it outside its kind's origins, can follow a variable, and a value equal to its
    variable's default changes nothing."""
    defaults = _variable_defaults(variables, None)
    changed = {str(name) for name, value in supplied.items() if _text(value) != defaults.get(name)}
    if not changed:
        return FlowSecretBinding(outcomes={}, rewrites_secrets=False)
    flow = _Flow(_steps(actions), edges, variables, {}, template_cache)
    outcomes: dict[str, list[OriginOutcome]] = {}
    for step_id in _function_steps(flow):
        action = flow.actions[step_id]
        config = flow.secret_config(step_id)
        keys = _secret_keys(action, template_cache)
        unbound = [
            key
            for key, origins in secret_origins(
                config.code, config.inputs, mapping_inputs(config.mappings), keys
            ).items()
            if origins is UNBOUND and key not in entered.get(step_id, set())
        ]
        connections = [
            outcome
            for outcome in integration_origin_outcomes(team_id, config, lambda: True)
            if outcome.binding == SecretBinding.KEPT
        ]
        if not (unbound or connections) or not flow.reads_any(step_id, changed):
            continue
        labels = _labels(config.inputs_schema)
        outcomes[step_id] = [
            *(
                OriginOutcome(
                    key=key,
                    check=OriginCheck.SECRET,
                    binding=SecretBinding.REFUSED,
                    message=secret_reentry_message(key, UNBOUND, UNBOUND, labels),
                )
                for key in sorted(unbound)
            ),
            *(dataclasses.replace(outcome, binding=SecretBinding.REFUSED) for outcome in connections),
        ]
    return FlowSecretBinding(outcomes=outcomes, rewrites_secrets=False)
