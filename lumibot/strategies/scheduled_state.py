import datetime
import json
import math
from decimal import Decimal

TYPE_KEY = "__lumibot_type__"
VALUE_KEY = "value"
ESCAPED_DICT_TYPE = "__lumibot_literal_dict__"
MAX_STATE_BYTES = 65536
MAX_STATE_DEPTH = 16
MAX_STATE_NODES = 4096

SENSITIVE_KEY_MARKERS = (
    "api_key",
    "apikey",
    "access_key",
    "secret",
    "token",
    "password",
    "passwd",
    "credential",
    "oauth",
    "private_key",
    "session",
    "authorization",
    "cookie",
    "set_cookie",
    "email",
    "username",
    "account_id",
    "account_number",
    "client_id",
    "refresh",
    "bearer",
)


def _stable_encoded_key(value):
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def encode_variable_for_backup(value):
    if isinstance(value, datetime.datetime):
        return {TYPE_KEY: "datetime", VALUE_KEY: value.isoformat()}
    if isinstance(value, datetime.date):
        return {TYPE_KEY: "date", VALUE_KEY: value.isoformat()}
    if isinstance(value, Decimal):
        return {TYPE_KEY: "decimal", VALUE_KEY: str(value)}
    if isinstance(value, dict):
        if set(value.keys()) == {TYPE_KEY, VALUE_KEY}:
            return {
                TYPE_KEY: ESCAPED_DICT_TYPE,
                VALUE_KEY: [[key, encode_variable_for_backup(nested)] for key, nested in value.items()],
            }
        return {key: encode_variable_for_backup(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return {
            TYPE_KEY: "tuple",
            VALUE_KEY: [encode_variable_for_backup(nested) for nested in value],
        }
    if isinstance(value, list):
        return [encode_variable_for_backup(nested) for nested in value]
    if isinstance(value, set):
        encoded_items = [encode_variable_for_backup(nested) for nested in value]
        return {
            TYPE_KEY: "set",
            VALUE_KEY: sorted(encoded_items, key=_stable_encoded_key),
        }
    return value


def decode_variable_from_backup(value):
    if isinstance(value, dict):
        if set(value.keys()) == {TYPE_KEY, VALUE_KEY}:
            value_type = value[TYPE_KEY]
            raw_value = value[VALUE_KEY]
            if value_type == "datetime":
                return datetime.datetime.fromisoformat(raw_value)
            if value_type == "date":
                return datetime.datetime.strptime(raw_value, "%Y-%m-%d").date()
            if value_type == "decimal":
                return Decimal(raw_value)
            if value_type == "tuple":
                return tuple(decode_variable_from_backup(nested) for nested in raw_value)
            if value_type == "set":
                return {decode_variable_from_backup(nested) for nested in raw_value}
            if value_type == ESCAPED_DICT_TYPE:
                if not isinstance(raw_value, list):
                    raise ValueError("Escaped variables dict must contain key/value pairs")
                return {key: decode_variable_from_backup(nested) for key, nested in raw_value}
        return {key: decode_variable_from_backup(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [decode_variable_from_backup(nested) for nested in value]
    return value


def _json_default(obj):
    if hasattr(obj, "to_dict"):
        return encode_variable_for_backup(obj.to_dict())
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return {TYPE_KEY: "decimal", VALUE_KEY: str(obj)}
    if isinstance(obj, set):
        return encode_variable_for_backup(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def validate_scheduled_state(state):
    if not isinstance(state, dict):
        raise ValueError("Variables backup must contain a JSON object")

    sensitive_paths = []
    nodes_seen = 0
    max_depth = MAX_STATE_DEPTH
    max_nodes = MAX_STATE_NODES
    stack = [(state, "$", 0)]
    while stack:
        current, path, depth = stack.pop()
        nodes_seen += 1
        if nodes_seen > max_nodes:
            raise ValueError(f"Scheduled state has more than {max_nodes} JSON nodes")
        if depth > max_depth:
            raise ValueError(f"Scheduled state exceeds max depth {max_depth}")

        if isinstance(current, dict):
            for key, nested in current.items():
                child_path = f"{path}.{key}"
                if any(marker in str(key).lower() for marker in SENSITIVE_KEY_MARKERS):
                    sensitive_paths.append(child_path)
                stack.append((nested, child_path, depth + 1))
        elif isinstance(current, list):
            for index, nested in enumerate(current):
                stack.append((nested, f"{path}[{index}]", depth + 1))
        elif isinstance(current, float):
            if not math.isfinite(current):
                raise ValueError(f"Scheduled state contains non-finite number at {path}")
        elif current is None or isinstance(current, (str, int, bool)):
            continue
        else:
            raise ValueError(f"Scheduled state contains non-JSON value at {path}")

    if sensitive_paths:
        preview = ", ".join(sensitive_paths[:5])
        raise ValueError(
            "Scheduled state contains sensitive-looking keys; store broker/API secrets in Secrets Manager, "
            f"not bot state. Refusing to persist paths: {preview}"
        )


def validate_scheduled_state_json(json_data):
    state_size = len(json_data.encode("utf-8"))
    max_bytes = MAX_STATE_BYTES
    if state_size > max_bytes:
        raise ValueError(f"Scheduled state is {state_size} bytes, above max {max_bytes} bytes")
    validate_scheduled_state(json.loads(json_data))


def serialize_variables_for_backup(variables):
    state_json = json.dumps(
        encode_variable_for_backup(variables),
        sort_keys=True,
        default=_json_default,
        allow_nan=False,
    )
    validate_scheduled_state_json(state_json)
    return state_json


def deserialize_variables_from_backup(json_data):
    validate_scheduled_state_json(json_data)
    data = json.loads(json_data)
    return {key: decode_variable_from_backup(value) for key, value in data.items()}
