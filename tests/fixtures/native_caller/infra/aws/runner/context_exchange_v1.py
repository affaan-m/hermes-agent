"""Versioned fixed-worker bootstrap. No imports or network activity at import time.

Protocol/state-machine and literal CLI integration follow the bridge API freeze.
This module is not yet a deployable entrypoint. All arguments come from trusted
image/launcher composition, never from worker request JSON.
"""
from collections.abc import Mapping
import re
from ..exchange import protocol as wire


class WorkerError(ValueError):
    """Sanitized fixed-worker rejection; never include credential data."""


_RELATIVE = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
_ALLOWED_AWS = {_RELATIVE, "AWS_EXECUTION_ENV", "AWS_REGION", "AWS_DEFAULT_REGION"}


def ecs_environment(environ):
    """Return only the ECS relative credential path, without a fallback chain."""
    if not isinstance(environ, Mapping):
        raise WorkerError("invalid worker environment")
    for key in environ:
        if not isinstance(key, str):
            raise WorkerError("invalid worker environment")
        if key.startswith("AWS_") and key not in _ALLOWED_AWS:
            raise WorkerError("alternate AWS credential configuration denied")
    relative = environ.get(_RELATIVE)
    if not isinstance(relative, str) or re.fullmatch(
        r"/v2/credentials/[A-Za-z0-9-]{1,128}", relative
    ) is None:
        raise WorkerError("invalid ECS relative credential path")
    return {_RELATIVE: relative}


def ecs_signing_provider(environ, *, container_provider_factory, crt_auth):
    """Bridge the standard ECS-only provider to the standard CRT signer.

    Production composition supplies botocore.credentials.ContainerProvider and
    awscrt.auth. No Session, default chain, profile or custom signature is used.
    Provider retrieval is intentionally lazy until the signer requests it.
    """
    selected = ecs_environment(environ)
    provider = container_provider_factory(environ=selected)

    def get_credentials():
        try:
            current = provider.load()
            if current is None:
                raise WorkerError("ECS credentials unavailable")
            frozen = current.get_frozen_credentials()
            if not all(isinstance(value, str) and value for value in (
                frozen.access_key, frozen.secret_key, frozen.token
            )):
                raise WorkerError("ECS temporary credentials unavailable")
            # Each delegate call loads fresh ECS credentials. Nothing is cached
            # by this wrapper or written to disk; standard SDK signs the WSS.
            return crt_auth.AwsCredentials(
                frozen.access_key, frozen.secret_key, frozen.token
            )
        except Exception:
            raise WorkerError("ECS credentials unavailable") from None

    return crt_auth.AwsCredentialsProvider.new_delegate(get_credentials)


def worker_topics(role_id, task_id):
    if not isinstance(role_id, str) or re.fullmatch(r"AROA[A-Z0-9]{17}", role_id) is None:
        raise WorkerError("invalid pinned task role identity")
    if not isinstance(task_id, str) or re.fullmatch(r"[0-9a-f]{32}", task_id) is None:
        raise WorkerError("invalid registered task identity")
    client_id = role_id + ":" + task_id
    return client_id, wire.TOPIC_PREFIX + "/request/" + client_id, wire.TOPIC_PREFIX + "/reply/" + client_id


def build_worker_client(*, endpoint, region, role_id, task_id,
                        credentials_provider, mqtt5, builder, on_publish,
                        on_connected, on_disconnected, on_stopped):
    """Construct a stopped MQTT5 client; caller owns lifecycle and bounded RPC.

    Endpoint/role/task values must be pinned by the trusted launcher. Topic
    metadata is not worker authentication; the broker and host verify it.
    """
    if region != "us-east-1" or not isinstance(endpoint, str) or re.fullmatch(
        r"[a-z0-9]+-ats\.iot\.us-east-1\.amazonaws\.com", endpoint
    ) is None:
        raise WorkerError("invalid pinned exchange endpoint")
    client_id, _, _ = worker_topics(role_id, task_id)
    if credentials_provider is None:
        raise WorkerError("explicit ECS signing provider required")
    return builder.websockets_with_default_aws_signing(
        endpoint=endpoint, region=region, credentials_provider=credentials_provider,
        client_id=client_id, port=443,
        session_behavior=mqtt5.ClientSessionBehaviorType.CLEAN,
        session_expiry_interval_sec=0,
        offline_queue_behavior=mqtt5.ClientOperationQueueBehaviorType.FAIL_ALL_ON_DISCONNECT,
        maximum_packet_size=65536, will=None, http_proxy_options=None,
        enable_metrics_collection=False,
        on_publish_received=on_publish,
        on_lifecycle_connection_success=on_connected,
        on_lifecycle_disconnection=on_disconnected,
        on_lifecycle_connection_failure=on_disconnected,
        on_lifecycle_stopped=on_stopped,
    )


# Exact previously reviewed deterministic CLI closure; image filesystem must be read-only.
CLI_PINS = {'ito_desk_tools/__init__.py': 'c4fb6bcb14a6afa6678ae92fb68f180f5ee19908861ccc914f940d8d8623435f', 'ito_desk_tools/nonpricing/planner.py': '0793551a9fc9e0d1c9464597542c9633409049bf2cf4c51993b7f0d76a204f82', 'ito_desk_tools/nonpricing/__init__.py': 'bfb1a25900e69cbc04c73037c2d9373e4881c5381d7c41b16d4333be1ec1bddb', 'ito_desk_tools/nonpricing/cli.py': '3d8650798bae969e387188a0574471aeae6f61118512f8013bd7b327e2ebef2f', 'ito_desk_tools/nonpricing/workflow.py': '55e505e02a6ded7388a6678538643b54996824494d71c50fa2dab9a47bbc00c4', 'scripts/ito-nonpricing': '78afa6ffdf50921e623cd29a2d2960c7911a6fc9092249d7e320c11888cfa0e5'}


class _OwnedScratch:
    # No destructor: unresolved process ownership must not erase its evidence.
    def __init__(self, root):
        import tempfile
        self.name = tempfile.mkdtemp(prefix="context-", dir=root)
    def cleanup(self):
        import shutil
        shutil.rmtree(self.name)


class PreparedCLI:
    """Prepared fixed context command, then one immediate spawn and bounded reap.

    Paths/runtime are trusted image composition. Input/projection come only from
    an authenticated, commitment-checked exchange. No worker JSON chooses argv.
    The shared StartGate must call start() directly; start() itself is no grant.
    """
    def __init__(self, *, source_root, python_path, scratch_root, input_bytes,
                 binding, request_spec, typed_payload):
        import hashlib
        import json
        from pathlib import Path
        import os
        import stat
        import tempfile
        from .context_results import _validate_binding, _validate_spec
        self._temp = None
        self._process = None
        self._used = False
        self._finished = False
        self._result = None
        def encoded(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=True, allow_nan=False).encode("ascii")
        try:
            _validate_binding(binding)
            _validate_spec(request_spec, binding)
            if (type(input_bytes) is not bytes or not 1 <= len(input_bytes) <= 1048576
                    or len(input_bytes) != request_spec["input_bytes"]
                    or hashlib.sha256(input_bytes).hexdigest() != binding["input_sha256"]):
                raise WorkerError("input commitment rejected")
            expected = {"schema", "operation", "task_id", "request_spec_sha256", "timeout"}
            if (type(typed_payload) is not dict or set(typed_payload) != expected
                    or type(typed_payload["schema"]) is not int or typed_payload["schema"] != 1
                    or typed_payload["operation"] != "nonpricing.context"
                    or typed_payload["request_spec_sha256"] != binding["request_spec_sha256"]
                    or type(typed_payload["task_id"]) is not str
                    or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", typed_payload["task_id"]) is None
                    or type(typed_payload["timeout"]) is not int
                    or not 1 <= typed_payload["timeout"] <= 30
                    or hashlib.sha256(encoded(typed_payload)).hexdigest() != binding["spec_sha256"]):
                raise WorkerError("typed command commitment rejected")
            if hashlib.sha256(encoded(CLI_PINS)).hexdigest() != binding["source_sha256"]:
                raise WorkerError("CLI closure commitment rejected")
            root = Path(source_root)
            if not root.is_absolute() or root.is_symlink() or not root.is_dir():
                raise WorkerError("invalid fixed source root")
            for name, pin in CLI_PINS.items():
                path = root / name
                if any(part.is_symlink() for part in (path, *path.parents) if part != root.parent):
                    raise WorkerError("symlink in CLI closure")
                digest = hashlib.sha256()
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise WorkerError("nonregular CLI source")
                    while True:
                        chunk = stream.read(65536)
                        if not chunk:
                            break
                        digest.update(chunk)
                if digest.hexdigest() != pin:
                    raise WorkerError("CLI source drift")
            runtime = Path(python_path)
            scratch = Path(scratch_root)
            if (not runtime.is_absolute() or not runtime.is_file()
                    or not scratch.is_absolute() or scratch.is_symlink()
                    or not scratch.is_dir()):
                raise WorkerError("invalid fixed process paths")
            # Source is pinned; runtime/image identity remains a launcher proof.
            self._temp = _OwnedScratch(scratch)
            snapshot = Path(self._temp.name) / "input.json"
            fd = os.open(snapshot, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(input_bytes)
            os.chmod(snapshot, 0o400)
            projection = request_spec["projection"]
            self._argv = ["/bin/bash", "-p", str(root / "scripts/ito-nonpricing"), str(runtime),
                          "context", "--snapshot", str(snapshot), "--company", projection["company_id"],
                          "--now", str(projection["evaluation_time"]), "--limit", str(projection["limit"])]
            self._timeout = typed_payload["timeout"]
            self._deadline_at = request_spec["deadline_at"]
            self._stdout_limit = request_spec["result_max_bytes"]
        except Exception:
            if self._temp is not None:
                self._temp.cleanup()
            raise WorkerError("CLI preparation rejected") from None

    def start(self, *, execution_deadline_at):
        import math
        import subprocess
        import time
        if self._used:
            raise WorkerError("CLI start already consumed")
        self._used = True
        wall = time.time()
        if (type(execution_deadline_at) not in (int, float)
                or not math.isfinite(execution_deadline_at)
                or not wall < execution_deadline_at <= self._deadline_at):
            raise WorkerError("execution deadline rejected")
        self._started = time.monotonic()
        self._stop_at = self._started + min(self._timeout, execution_deadline_at - wall)
        try:
            self._process = subprocess.Popen(
                self._argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=self._temp.name,
                env={"PATH": "/usr/bin:/bin"}, close_fds=True, start_new_session=True)
        except Exception:
            raise WorkerError("CLI launch unknown") from None
        # Keep the Popen/reaping capability private to this one owner.
        return None

    def _reap(self):
        import os
        import signal
        process = self._process
        if process is None:
            return True
        clean = True
        if process.returncode is None:
            try:
                # WNOWAIT verifies current child ownership without releasing its
                # PID. No other code gets the Popen object or reaps this child.
                observed = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
            except (ChildProcessError, OSError):
                return False  # Unknown ownership: retain process and scratch.
            if observed is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError:
                    clean = False
            # For this pinned exec-only CLI, terminal leader observation means
            # its only child has exited. Reap it without a needless group signal
            # (macOS rejects signals to a zombie-only group with EPERM). This is
            # deliberately not a supervisor for arbitrary descendant programs.
            try:
                process.wait(timeout=1)
            except Exception:
                clean = False
        # If another owner already reaped it, never signal its historical PGID.
        # This fixed CLI execs Python; arbitrary escaping descendants are outside
        # its allowed source contract and are not claimed to be contained here.
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    clean = False
        return clean and process.returncode is not None

    def finish(self):
        import hashlib
        import os
        import selectors
        import time
        if self._process is None or self._finished:
            raise WorkerError("CLI result unavailable")
        self._finished = True
        process = self._process
        selector = None
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        limits = {"stdout": self._stdout_limit, "stderr": 8192}
        complete, timed_out = True, False
        try:
            selector = selectors.DefaultSelector()
            for stream, name in ((process.stdout, "stdout"), (process.stderr, "stderr")):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = self._stop_at - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    complete = False
                    break
                for key, _ in selector.select(min(remaining, .05)):
                    name = key.data
                    chunk = os.read(key.fileobj.fileno(), min(4096, limits[name] - len(buffers[name]) + 1))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    if len(buffers[name]) + len(chunk) > limits[name]:
                        complete = False
                        break
                    buffers[name].extend(chunk)
                if not complete:
                    break
            if complete:
                # Observe terminal status without reaping the group leader.
                # Destructive cleanup below precedes the sole process.wait().
                while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
                    remaining = self._stop_at - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        complete = False
                        break
                    time.sleep(min(.005, remaining))
        except Exception:
            complete = False
        finally:
            try:
                if selector is not None:
                    try:
                        selector.close()
                    except Exception:
                        complete = False
            finally:
                cleanup = self._reap()
        duration = time.monotonic() - self._started
        stdout, stderr = bytes(buffers["stdout"]), bytes(buffers["stderr"])
        code = process.returncode
        if code is None:
            code = 255
        elif code < 0:
            code = 128 - code
        summary = dict(exit_code=code, timed_out=timed_out, cleanup_complete=cleanup,
                       capture_complete=complete, stdout_bytes=len(stdout), stderr_bytes=len(stderr),
                       stdout_sha256=hashlib.sha256(stdout).hexdigest(),
                       stderr_sha256=hashlib.sha256(stderr).hexdigest(), duration_s=duration)
        # Incomplete capture counts/digests describe retained bytes only. This
        # summary is failed evidence, never a successful original-output claim.
        self._result = {"stdout": stdout, "result": summary}
        return self._result

    def close(self):
        # Retry unresolved cleanup while retaining the child/scratch. _reap
        # verifies ownership and never signals a previously reaped leader.
        if not self._reap():
            return False
        if self._temp is not None:
            try:
                self._temp.cleanup()
            except OSError:
                return False
            self._temp = None
        return True


class InputAssembly:
    """One bounded authenticated FETCH response, no cross-RPC continuation."""
    def __init__(self, fetch_frame, expected_binding, *, clock):
        from .context_results import _validate_binding
        frame = wire.parse_request(wire.encode(fetch_frame))
        if frame["verb"] != "FETCH":
            raise WorkerError("input request rejected")
        _validate_binding(expected_binding)
        if (frame["request_id"] != expected_binding["request_id"]
                or frame["manifest_sha256"] != expected_binding["manifest_sha256"]):
            raise WorkerError("input binding rejected")
        self._frame = frame
        self._binding = wire.detached(expected_binding)
        self._clock = clock
        self._at = wire.number(clock())
        self._buffer = bytearray()
        self._sequence = 0
        self._spec = None
        self._state = "receiving"

    def disconnect(self):
        self._state = "held"
        self._buffer.clear()

    def feed(self, authenticated_bytes):
        import base64
        import hashlib
        from .context_results import _validate_spec
        try:
            if self._state != "receiving" or not self._at <= wire.number(self._clock()) < self._at+2:
                raise WorkerError("input response expired")
            frame = wire.parse(authenticated_bytes)
            wire._common(frame)
            expected = wire.detached(self._frame)
            expected.update(verb="INPUT", data=frame["data"])
            if wire.encode(frame) != wire.encode(expected):
                raise WorkerError("input response binding rejected")
            data = frame["data"]
            wire.fields(data, {"sequence", "total_chunks", "bytes_b64", "chunk_sha256",
                               "input_sha256", "binding", "request_spec"})
            if wire.encode(data["binding"]) != wire.encode(self._binding):
                raise WorkerError("input binding rejected")
            spec = data["request_spec"]
            _validate_spec(spec, self._binding)
            if not 1 <= spec["input_bytes"] <= wire.INPUT_MAX:
                raise WorkerError("input size rejected")
            total = (spec["input_bytes"] + wire.CHUNK_MAX - 1)//wire.CHUNK_MAX
            if (type(data["sequence"]) is not int or data["sequence"] != self._sequence
                    or type(data["total_chunks"]) is not int or data["total_chunks"] != total
                    or data["input_sha256"] != self._binding["input_sha256"]
                    or (self._spec is not None and wire.encode(spec) != wire.encode(self._spec))):
                raise WorkerError("input sequence rejected")
            text = data["bytes_b64"]
            if type(text) is not str or not 0 < len(text) <= 4*((wire.CHUNK_MAX+2)//3):
                raise WorkerError("input chunk rejected")
            chunk = base64.b64decode(text, validate=True)
            length = min(wire.CHUNK_MAX, spec["input_bytes"]-len(self._buffer))
            if (len(chunk) != length or base64.b64encode(chunk).decode("ascii") != text
                    or hashlib.sha256(chunk).hexdigest() != data["chunk_sha256"]):
                raise WorkerError("input chunk digest rejected")
            self._buffer.extend(chunk)
            self._spec = wire.detached(spec)
            self._sequence += 1
            if self._sequence == total:
                if hashlib.sha256(self._buffer).hexdigest() != self._binding["input_sha256"]:
                    raise WorkerError("input digest rejected")
                self._state = "complete"
                return True
            return False
        except Exception:
            self.disconnect()
            raise WorkerError("input stream rejected") from None

    def take(self):
        if self._state != "complete":
            raise WorkerError("input incomplete")
        self._state = "consumed"
        result = {"input_bytes": bytes(self._buffer), "request_spec": self._spec}
        self._buffer.clear()
        self._spec = None
        return result


class WorkerAttempt:
    """One fixed request through an injected authenticated, bounded duplex link.

    Link.send(bytes) uses QoS0 without offline queue; receive(deadline_mono)
    returns only exact-topic, nonretained, authenticated response bytes.
    Link.set_disconnect_callback(callback) immediately invalidates pending input
    and start. No SDK/network implementation is implied by this source adapter.
    """
    def __init__(self, *, attempt_id, binding, typed_payload, source_root,
                 python_path, scratch_root, clock, wall_clock):
        from .context_results import _validate_binding
        _validate_binding(binding)
        self.attempt_id = wire.hex_id(attempt_id)
        self.binding = wire.detached(binding)
        self.typed = wire.detached(typed_payload)
        self._paths = dict(source_root=source_root,python_path=python_path,scratch_root=scratch_root)
        self._clock, self._wall = clock, wall_clock
        self._used = False
        self._generation = self._invocation = None
        self._gate = self._assembly = self._prepared = None
        self._result = self._spec = None
        self._winning_invocation = None
        self._evidence_reconnect_used = False

    def _frame(self, verb, data):
        import uuid
        value = dict(schema=wire.SCHEMA,attempt_id=self.attempt_id,
                     request_id=self.binding["request_id"],manifest_sha256=self.binding["manifest_sha256"],
                     connection_generation=self._generation,invocation_id=self._invocation,
                     rpc_id=uuid.uuid4().hex,verb=verb,data=data)
        return wire.parse_request(wire.encode(value))

    def _disconnect(self):
        if self._gate is not None:
            self._gate.disconnect()
        if self._assembly is not None:
            self._assembly.disconnect()
        # An already-started fixed child finishes under its original deadline.
        # Discarding a transport never claims that the provider was cancelled.

    def _reply(self, link, request, verb, started):
        if not started <= wire.number(self._clock()) < started+2:
            raise WorkerError("response expired")
        value = wire.parse(link.receive(started+2))
        wire._common(value)
        if not started <= wire.number(self._clock()) < started+2:
            raise WorkerError("response expired")
        expected = wire.detached(request)
        expected.update(verb=verb,data=value["data"])
        if verb == "WELCOME":
            expected["connection_generation"] = wire.hex_id(value["connection_generation"])
            expected["invocation_id"] = wire.hex_id(value["invocation_id"])
        if wire.encode(value) != wire.encode(expected):
            raise WorkerError("response binding rejected")
        return value

    def _hello(self, link):
        import uuid
        self._generation = self._invocation = None
        frame = self._frame("HELLO", {"boot_nonce":uuid.uuid4().hex})
        at = wire.number(self._clock())
        link.send(wire.encode(frame))
        reply = self._reply(link,frame,"WELCOME",at)
        wire.fields(reply["data"],{"boot_nonce","mode"})
        if (reply["data"]["boot_nonce"] != frame["data"]["boot_nonce"]
                or reply["data"]["mode"] not in ("ready","evidence_only")):
            raise WorkerError("welcome rejected")
        self._generation,self._invocation = reply["connection_generation"],reply["invocation_id"]
        return reply["data"]["mode"]

    def run(self, link):
        import uuid
        if self._used:
            raise WorkerError("attempt already consumed")
        self._used = True
        link.set_disconnect_callback(self._disconnect)
        try:
            if self._hello(link) != "ready":
                return {"state":"held","reason":"evidence_only_without_local_result"}
            fetch = self._frame("FETCH",{"reservation_id":uuid.uuid4().hex})
            self._assembly = InputAssembly(fetch,self.binding,clock=self._clock)
            at = wire.number(self._clock())
            link.send(wire.encode(fetch))
            while True:
                response = link.receive(at+2)
                if self._assembly.feed(response):
                    break
            assembled = self._assembly.take()
            self._assembly = None
            self._spec = assembled["request_spec"]
            self._prepared = PreparedCLI(**self._paths,input_bytes=assembled["input_bytes"],
                                         binding=self.binding,request_spec=self._spec,typed_payload=self.typed)
            del assembled
            ready = self._frame("READY_START",{key:self.binding[key] for key in ("input_sha256","source_sha256")})
            self._gate = wire.StartGate(ready,clock=self._clock,wall_clock=self._wall,
                                       canonical_deadline_at=self._spec["deadline_at"])
            at = wire.number(self._clock())
            link.send(wire.encode(ready))
            command = link.receive(at+2)
            self._gate.execute(command,lambda deadline:self._prepared.start(execution_deadline_at=deadline))
            self._winning_invocation = self._invocation
            self._result = self._prepared.finish()
            if self._prepared.close() is not True:
                self._result["result"]["cleanup_complete"] = False
            return self._publish_evidence(link)
        except Exception:
            self._disconnect()
            return {"state":"evidence_pending" if self._result is not None else "unknown"}
        finally:
            if self._prepared is not None:
                self._prepared.close()

    def _publish_evidence(self, link):
        import base64
        import hashlib
        if (self._result is None or self._spec is None
                or not wire.number(self._wall()) < self._spec["deadline_at"]+60
                or self._invocation != self._winning_invocation):
            raise WorkerError("evidence unavailable")
        stdout = self._result["stdout"]
        summary = wire.validate_summary(self._result["result"])
        if summary["capture_complete"] and stdout:
            frame = self._frame("RESULT",{"stdout_b64":base64.b64encode(stdout).decode("ascii"),
                                        "stdout_sha256":hashlib.sha256(stdout).hexdigest()})
            at = wire.number(self._clock());link.send(wire.encode(frame))
            reply = self._reply(link,frame,"OBSERVED",at)
            wire.fields(reply["data"],{"stdout_sha256","fresh"})
            if (type(reply["data"]["fresh"]) is not bool
                    or reply["data"]["stdout_sha256"] != summary["stdout_sha256"]):
                raise WorkerError("result acknowledgement rejected")
        frame = self._frame("PROCESS_SUMMARY",{"summary":summary})
        at = wire.number(self._clock());link.send(wire.encode(frame))
        reply = self._reply(link,frame,"SUMMARY_OBSERVED",at)
        wire.fields(reply["data"],{"summary_sha256","fresh"})
        if (type(reply["data"]["fresh"]) is not bool
                or reply["data"]["summary_sha256"] != hashlib.sha256(wire.encode(summary)).hexdigest()):
            raise WorkerError("summary acknowledgement rejected")
        return {"state":"evidence_sent","process_success":wire.successful_summary(summary)}

    def reconnect_evidence(self, link):
        """One explicit evidence-only reconnect, never execution recovery.

        Host must attach the new link to a reconstructed controller from the
        original canonical attempt. No automatic replay or start gate is built.
        """
        if self._evidence_reconnect_used or self._result is None or self._winning_invocation is None:
            raise WorkerError("evidence reconnect unavailable")
        self._evidence_reconnect_used = True
        link.set_disconnect_callback(self._disconnect)
        try:
            if self._hello(link) != "evidence_only" or self._invocation != self._winning_invocation:
                raise WorkerError("evidence reconnect rejected")
            return self._publish_evidence(link)
        except Exception:
            return {"state":"evidence_pending"}


class MqttLink:
    """AWS SDK adapter with one live session and bounded application callbacks.

    The SDK and TLS/IAM endpoint are trusted image/bootstrap dependencies.
    These controls do not prove SDK-internal allocation or AWS identity policy.
    A new link can carry evidence reconnect; an old link never becomes live again.
    """
    def __init__(self, *, endpoint, region, role_id, task_id, credentials_provider,
                 mqtt5, builder, clock):
        import queue
        import threading
        self._mqtt, self._clock = mqtt5, clock
        self._client_id,self._request_topic,self._reply_topic = worker_topics(role_id,task_id)
        self._queue = queue.Queue(maxsize=4)
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._held = self._connected = self._seen_connect = self._subscribed = False
        self._started = False
        self._callback = None
        self._client = build_worker_client(endpoint=endpoint,region=region,role_id=role_id,
            task_id=task_id,credentials_provider=credentials_provider,mqtt5=mqtt5,builder=builder,
            on_publish=self._published,on_connected=self._connection,
            on_disconnected=self._disconnected,on_stopped=lambda data:self._stopped.set())

    def set_disconnect_callback(self, callback):
        if not callable(callback):
            raise WorkerError("disconnect callback required")
        with self._lock:
            self._callback = callback
            held = self._held
        if held:
            callback()

    def _hold(self):
        import queue
        with self._lock:
            self._held = True
            self._connected = False
            self._ready.set()
            callback = self._callback
            while True:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
        if callback is not None:
            callback()

    def _disconnected(self, _data):
        self._hold()

    def _connection(self, data):
        try:
            negotiated = data.negotiated_settings
            with self._lock:
                if (self._held or self._seen_connect
                        or negotiated.client_id != self._client_id
                        or negotiated.rejoined_session is not False
                        or negotiated.session_expiry_interval_sec != 0):
                    raise WorkerError("session negotiation rejected")
                self._seen_connect = self._connected = True
                self._ready.set()
        except Exception:
            self._hold()

    def _published(self, data):
        try:
            packet = data.publish_packet
            payload = packet.payload
            if (packet.topic != self._reply_topic or packet.retain is not False
                    or packet.qos != self._mqtt.QoS.AT_MOST_ONCE
                    or type(payload) not in (bytes,bytearray)
                    or not 0 < len(payload) <= wire.MAX_FRAME):
                raise WorkerError("response packet rejected")
            with self._lock:
                if self._held or not self._connected or not self._subscribed:
                    raise WorkerError("response session rejected")
                self._queue.put_nowait(bytes(payload))
        except Exception:
            self._hold()

    def connect(self, *, timeout=5):
        if type(timeout) not in (int,float) or not 0 < timeout <= 5 or self._started:
            raise WorkerError("connection attempt rejected")
        self._started = True
        at = wire.number(self._clock())
        try:
            self._client.start()
            if not self._ready.wait(timeout):
                raise WorkerError("connection timed out")
            with self._lock:
                if self._held or not self._connected:
                    raise WorkerError("connection rejected")
            remaining = at+timeout-wire.number(self._clock())
            if remaining <= 0:
                raise WorkerError("connection timed out")
            request = self._mqtt.SubscribePacket(subscriptions=[self._mqtt.Subscription(
                topic_filter=self._reply_topic,qos=self._mqtt.QoS.AT_MOST_ONCE,
                retain_as_published=True,retain_handling_type=self._mqtt.RetainHandlingType.DONT_SEND)])
            reply = self._client.subscribe(request).result(timeout=min(2,remaining))
            if list(reply.reason_codes) != [self._mqtt.SubackReasonCode.GRANTED_QOS_0]:
                raise WorkerError("reply subscription rejected")
            with self._lock:
                now = wire.number(self._clock())
                if self._held or not self._connected or not at <= now < at+timeout:
                    raise WorkerError("connection changed")
                self._subscribed = True
        except Exception:
            self._hold()
            raise WorkerError("connection unavailable") from None

    def send(self, body):
        wire.parse_request(body)
        try:
            with self._lock:
                if self._held or not self._connected or not self._subscribed:
                    raise WorkerError("transport held")
            packet = self._mqtt.PublishPacket(payload=body,topic=self._request_topic,
                qos=self._mqtt.QoS.AT_MOST_ONCE,retain=False,message_expiry_interval_sec=2)
            self._client.publish(packet).result(timeout=2)
            with self._lock:
                if self._held:
                    raise WorkerError("publication unknown")
        except Exception:
            self._hold()
            raise WorkerError("publication unknown") from None

    def receive(self, deadline):
        import queue
        deadline = wire.number(deadline)
        while True:
            with self._lock:
                if self._held or not self._connected:
                    raise WorkerError("transport held")
            remaining = deadline-wire.number(self._clock())
            if remaining <= 0:
                self._hold()
                raise WorkerError("response timed out")
            try:
                body = self._queue.get(timeout=min(remaining,.05))
            except queue.Empty:
                continue
            with self._lock:
                if self._held:
                    raise WorkerError("transport held")
            if wire.number(self._clock()) >= deadline:
                self._hold()
                raise WorkerError("response timed out")
            return body

    def close(self):
        self._hold()
        try:
            self._client.stop()
        except Exception:
            return False
        return self._stopped.wait(2)
