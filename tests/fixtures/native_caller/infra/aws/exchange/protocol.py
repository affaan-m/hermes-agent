"""Closed source-only exchange protocol. No signing, credentials or network calls."""
from __future__ import annotations
import base64
import hashlib
import json
import math
import re
import threading

SCHEMA = 2
TOPIC_PREFIX = "ito-context/v2"
MAX_FRAME = 65536
INPUT_MAX = 1048576
RESULT_MAX = 8192
CHUNK_MAX = 32768
BOOTSTRAP_MAX = 4096
BOOTSTRAP_INFO_MAX = 8192
BOOTSTRAP_COORDINATES = {"request_spec_sha256", "spec_sha256", "input_sha256",
                         "source_sha256", "canonical_sha256", "launch_token"}
COMMON = {"schema", "attempt_id", "request_id", "connection_generation",
          "invocation_id", "rpc_id", "verb", "manifest_sha256", "data"}


class ExchangeError(Exception):
    """Fixed public error codes only; never wrap private exception text."""


def fields(value, names):
    if type(value) is not dict or set(value) != set(names):
        raise ExchangeError("invalid_fields")


def hex_id(value, size=32):
    if type(value) is not str or re.fullmatch("[0-9a-f]{"+str(size)+"}", value) is None:
        raise ExchangeError("invalid_identity")
    return value


def number(value):
    if type(value) not in (int, float):
        raise ExchangeError("invalid_time")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        raise ExchangeError("invalid_time") from None
    if not finite or value < 0:
        raise ExchangeError("invalid_time")
    return value


def encode(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode("ascii")
    except Exception:
        raise ExchangeError("invalid_json") from None


def detached(value):
    return json.loads(encode(value))


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ExchangeError("duplicate_field")
        result[key] = value
    return result


def _constant(_):
    raise ExchangeError("nonfinite_number")


def parse(data):
    if type(data) is not bytes or not 0 < len(data) <= MAX_FRAME:
        raise ExchangeError("frame_size")
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=_constant)
        encode(value)
        return value
    except Exception:
        raise ExchangeError("invalid_json") from None


def _common(frame):
    fields(frame, COMMON)
    if type(frame["schema"]) is not int or frame["schema"] != SCHEMA:
        raise ExchangeError("wrong_protocol")
    for key in ("attempt_id", "request_id", "rpc_id"):
        hex_id(frame[key])
    if not (frame["verb"] == "BOOTSTRAP" and frame["manifest_sha256"] is None):
        hex_id(frame["manifest_sha256"], 64)
    if type(frame["verb"]) is not str or type(frame["data"]) is not dict:
        raise ExchangeError("invalid_frame")


def decode_stdout(data):
    fields(data, {"stdout_b64", "stdout_sha256"})
    hex_id(data["stdout_sha256"], 64)
    text = data["stdout_b64"]
    if type(text) is not str or not 0 < len(text) <= 4*((RESULT_MAX+2)//3):
        raise ExchangeError("result_size")
    try:
        value = base64.b64decode(text, validate=True)
    except Exception:
        raise ExchangeError("result_encoding") from None
    if (not 0 < len(value) <= RESULT_MAX
            or base64.b64encode(value).decode("ascii") != text
            or hashlib.sha256(value).hexdigest() != data["stdout_sha256"]):
        raise ExchangeError("result_digest")
    return value


SUMMARY_FIELDS = {"exit_code", "timed_out", "cleanup_complete", "capture_complete",
                  "stdout_bytes", "stderr_bytes", "stdout_sha256", "stderr_sha256",
                  "duration_s"}


def validate_summary(value):
    """Bounded worker assertions, never independently verified process telemetry."""
    fields(value, SUMMARY_FIELDS)
    if type(value["exit_code"]) is not int or not -(2**31) <= value["exit_code"] < 2**31:
        raise ExchangeError("summary_exit")
    for key in ("timed_out", "cleanup_complete", "capture_complete"):
        if type(value[key]) is not bool:
            raise ExchangeError("summary_boolean")
    for stream in ("stdout", "stderr"):
        count, digest = value[stream+"_bytes"], value[stream+"_sha256"]
        if type(count) is not int or not 0 <= count < 2**63:
            raise ExchangeError("summary_count")
        hex_id(digest, 64)
        if count == 0 and digest != hashlib.sha256(b"").hexdigest():
            raise ExchangeError("summary_empty_digest")
    if number(value["duration_s"]) > 180:
        raise ExchangeError("summary_duration")
    return detached(value)


def successful_summary(value):
    return (value["exit_code"] == 0 and not value["timed_out"]
            and value["cleanup_complete"] and value["capture_complete"])


def parse_request(data):
    frame = parse(data)
    _common(frame)
    verb = frame["verb"]
    if verb == "BOOTSTRAP":
        if (len(data) > BOOTSTRAP_MAX or frame["manifest_sha256"] is not None
                or frame["connection_generation"] is not None or frame["invocation_id"] is not None):
            raise ExchangeError("bootstrap_identity_or_size")
        fields(frame["data"], BOOTSTRAP_COORDINATES | {"boot_nonce"})
        for key, value in frame["data"].items():
            hex_id(value, 32 if key in ("launch_token", "boot_nonce") else 64)
    elif verb == "HELLO":
        if frame["connection_generation"] is not None or frame["invocation_id"] is not None:
            raise ExchangeError("hello_identity")
        fields(frame["data"], {"boot_nonce"})
        hex_id(frame["data"]["boot_nonce"])
    else:
        hex_id(frame["connection_generation"])
        hex_id(frame["invocation_id"])
        if verb == "FETCH":
            fields(frame["data"], {"reservation_id"})
            hex_id(frame["data"]["reservation_id"])
        elif verb == "READY_START":
            fields(frame["data"], {"input_sha256", "source_sha256"})
            for digest in frame["data"].values():
                hex_id(digest, 64)
        elif verb == "RESULT":
            decode_stdout(frame["data"])
        elif verb == "PROCESS_SUMMARY":
            fields(frame["data"], {"summary"})
            validate_summary(frame["data"]["summary"])
        else:
            raise ExchangeError("unsupported_verb")
    return frame


def validate_binding(binding):
    """Frozen eight-field consumer shape plus canonical executor text bounds."""
    fields(binding, {"request_id", "request_spec_sha256", "manifest_sha256",
                    "canonical_sha256", "spec_sha256", "input_sha256", "source_sha256", "executor"})
    for key in set(binding) - {"executor"}:
        hex_id(binding[key], 32 if key == "request_id" else 64)
    executor = binding["executor"]
    fields(executor, {"task_arn", "task_definition_arn", "launch_token"})
    hex_id(executor["launch_token"])
    for key in ("task_arn", "task_definition_arn"):
        value = executor[key]
        if (type(value) is not str or not 1 <= len(value) <= 512
                or any(ord(ch) < 32 for ch in value)):
            raise ExchangeError("executor_identity")
    return detached(binding)


def validate_typed_payload(payload, binding):
    """Same closed command commitment as the fixed PreparedCLI/helper contract."""
    fields(payload, {"schema", "operation", "task_id", "request_spec_sha256", "timeout"})
    if (type(payload["schema"]) is not int or payload["schema"] != 1
            or payload["operation"] != "nonpricing.context"
            or type(payload["task_id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", payload["task_id"]) is None
            or type(payload["timeout"]) is not int or not 1 <= payload["timeout"] <= 30
            or payload["request_spec_sha256"] != binding["request_spec_sha256"]
            or hashlib.sha256(encode(payload)).hexdigest() != binding["spec_sha256"]):
        raise ExchangeError("typed_payload_binding")
    return detached(payload)


def bootstrap_response(request, data):
    """Only this metadata response may omit generation/invocation; no authority."""
    frame = parse_request(encode(request))
    if frame["verb"] != "BOOTSTRAP":
        raise ExchangeError("bootstrap_request_required")
    fields(data, {"boot_nonce", "binding", "typed_payload", "deadline_at", "expires_at"})
    binding = validate_binding(data["binding"])
    validate_typed_payload(data["typed_payload"], binding)
    expected = {key: binding[key] for key in BOOTSTRAP_COORDINATES - {"launch_token"}}
    expected.update(launch_token=binding["executor"]["launch_token"], boot_nonce=data["boot_nonce"])
    if (frame["request_id"] != binding["request_id"] or frame["data"] != expected
            or number(data["expires_at"]) > number(data["deadline_at"])):
        raise ExchangeError("bootstrap_response_binding")
    frame.update(verb="BOOTSTRAP_INFO", data=detached(data), manifest_sha256=binding["manifest_sha256"])
    if len(encode(frame)) > BOOTSTRAP_INFO_MAX:
        raise ExchangeError("frame_size")
    return frame


def response(request, verb, data, *, generation, invocation):
    value = detached(request)
    value.update(verb=verb, data=detached(data),
                 connection_generation=hex_id(generation), invocation_id=hex_id(invocation))
    if len(encode(value)) > MAX_FRAME:
        raise ExchangeError("frame_size")
    return value


class StartGate:
    """Wrapper-side one-use delivery gate; arm only after local input/source checks.

    The trusted transport must authenticate the response before execute(). This
    helper has no authority to verify a broker or reconstruct a lost reservation.
    It consumes state BEFORE calling the injected immediate launcher.
    """
    def __init__(self, ready_request, *, clock, wall_clock, canonical_deadline_at, max_age=2.0):
        frame = parse_request(encode(ready_request))
        if frame["verb"] != "READY_START" or not callable(clock) or not callable(wall_clock):
            raise ExchangeError("start_gate_config")
        if type(max_age) not in (int, float) or not 0 < max_age <= 2:
            raise ExchangeError("start_gate_config")
        self._ready = frame
        self._clock = clock
        self._wall_clock = wall_clock
        self._deadline = number(canonical_deadline_at)
        self._at = number(clock())
        self._max_age = max_age
        self._state = "waiting"
        self._lock = threading.Lock()

    def disconnect(self):
        with self._lock:
            if self._state == "waiting":
                self._state = "held"

    def execute(self, authenticated_command, launch):
        with self._lock:
            if self._state != "waiting":
                raise ExchangeError("start_already_consumed")
            self._state = "held"
            now = number(self._clock())
            if not self._at <= now < self._at + self._max_age:
                raise ExchangeError("start_expired")
            command = parse(authenticated_command)
            _common(command)
            expected = detached(self._ready)
            expected["verb"] = "EXECUTE_NOW"
            fields(command["data"], {"input_sha256", "source_sha256", "execution_deadline_at"})
            deadline = number(command["data"]["execution_deadline_at"])
            expected["data"]["execution_deadline_at"] = deadline
            wall_now = number(self._wall_clock())
            if not wall_now < deadline <= min(self._deadline, wall_now+30):
                raise ExchangeError("execution_deadline")
            if command != expected or not callable(launch):
                raise ExchangeError("start_command_mismatch")
            self._state = "consumed"
        try:
            return launch(deadline)
        except Exception:
            raise ExchangeError("launch_unknown") from None
