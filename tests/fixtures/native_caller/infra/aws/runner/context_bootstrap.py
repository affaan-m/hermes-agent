"""Bounded worker bootstrap/native metadata. No SDK, environment or I/O at import."""
import re
from collections.abc import Mapping
from .context_exchange_v1 import WorkerError, MqttLink, WorkerAttempt
from .context_results import _validate_binding
from ..exchange import protocol as wire

COORDINATES = {"attempt_id", "request_id", "request_spec_sha256", "spec_sha256",
               "input_sha256", "source_sha256", "canonical_sha256", "launch_token"}


def parse_coordinates(raw):
    try:
        if type(raw) is not bytes or not 0 < len(raw) <= 4096:
            raise WorkerError("bootstrap coordinates rejected")
        value = wire.parse(raw)
        wire.fields(value, COORDINATES)
        for key, item in value.items():
            wire.hex_id(item, 32 if key in {"attempt_id", "request_id", "launch_token"} else 64)
        if wire.encode(value) != raw:
            raise WorkerError("bootstrap coordinates rejected")
        return value
    except Exception:
        raise WorkerError("bootstrap coordinates rejected") from None


def parse_native_metadata(raw, *, account, region, cluster_arn, family, container_name):
    try:
        if (type(account) is not str or re.fullmatch(r"[0-9]{12}",account) is None
                or region != "us-east-1" or type(family) is not str
                or re.fullmatch(r"[A-Za-z0-9_-]{1,255}",family) is None
                or type(container_name) is not str or not container_name):
            raise WorkerError("native metadata rejected")
        prefix = "arn:aws:ecs:"+region+":"+account+":"
        if (type(cluster_arn) is not str or not cluster_arn.startswith(prefix+"cluster/")
                or re.fullmatch(r"[A-Za-z0-9_-]{1,255}",cluster_arn.removeprefix(prefix+"cluster/")) is None):
            raise WorkerError("native metadata rejected")
        value = wire.parse(raw)
        if type(value) is not dict or value.get("Cluster") not in (cluster_arn,cluster_arn.rsplit("/",1)[1]) or value.get("Family") != family:
            raise WorkerError("native metadata rejected")
        task_arn, revision = value.get("TaskARN"), value.get("Revision")
        task_prefix = prefix+"task/"+cluster_arn.rsplit("/",1)[1]+"/"
        if (type(task_arn) is not str or not task_arn.startswith(task_prefix)
                or type(revision) is not str or re.fullmatch(r"[1-9][0-9]{0,8}",revision) is None):
            raise WorkerError("native metadata rejected")
        task_id = wire.hex_id(task_arn.removeprefix(task_prefix))
        containers = value.get("Containers")
        if (type(containers) is not list or not 1 <= len(containers) <= 8
                or any(type(item) is not dict or type(item.get("Type")) is not str for item in containers)):
            raise WorkerError("native metadata rejected")
        normal = [item for item in containers if item["Type"] == "NORMAL"]
        if len(normal) != 1:
            raise WorkerError("native metadata rejected")
        container = normal[0]
        if (container.get("Name") != container_name or type(container.get("Image")) is not str
                or not 0 < len(container["Image"]) <= 2048
                or type(container.get("ImageID")) is not str
                or re.fullmatch(r"sha256:[0-9a-f]{64}",container["ImageID"]) is None):
            raise WorkerError("native metadata rejected")
        return dict(task_arn=task_arn, task_id=task_id,
            task_definition_arn=prefix+"task-definition/"+family+":"+revision,
            image_digest=container["ImageID"], container_name=container_name)
    except Exception:
        raise WorkerError("native metadata rejected") from None


def read_native_metadata(environ, *, config, clock, deadline, connector=None):
    """Read only the fixed ECS v4 /task endpoint, with a total two-second budget.

    Raw HTTP is bounded before stdlib parsing. Direct fixed-IP sockets bypass
    proxies/redirects/DNS; every receive rechecks the absolute monotonic budget.
    Tests inject socket-shaped objects and never connect to the real endpoint.
    """
    sock = None
    try:
        if not isinstance(environ, Mapping):
            raise WorkerError("native metadata unavailable")
        uri = environ.get("ECS_CONTAINER_METADATA_URI_V4")
        if type(uri) is not str or re.fullmatch(
                r"http://169\.254\.170\.2/v4/[A-Za-z0-9-]{1,128}",uri) is None:
            raise WorkerError("native metadata unavailable")
        at = wire.number(clock())
        stop = min(wire.number(deadline),at+2)
        last = at
        def remaining():
            nonlocal last
            now = wire.number(clock())
            if not last <= now < stop:
                raise WorkerError("native metadata unavailable")
            last = now
            return stop-now
        if connector is None:
            import socket
            connector = socket.create_connection
        sock = connector(("169.254.170.2",80), timeout=remaining())
        sock.settimeout(remaining())
        path = uri.removeprefix("http://169.254.170.2")+"/task"
        sock.sendall(("GET "+path+" HTTP/1.1\r\nHost: 169.254.170.2\r\n"
                      "Accept: application/json\r\nConnection: close\r\n\r\n").encode("ascii"))
        chunks, size = [], 0
        while True:
            sock.settimeout(remaining())
            block = sock.recv(min(4096, 73729-size))
            remaining()
            if not block:
                break
            size += len(block)
            if size > 73728:
                raise WorkerError("native metadata unavailable")
            chunks.append(block)
        raw = b"".join(chunks)
        end = raw.find(b"\r\n\r\n")
        if not 0 <= end <= 8192:
            raise WorkerError("native metadata unavailable")
        import io
        import http.client
        class MemorySocket:
            def makefile(self, mode):return io.BytesIO(raw)
        response = http.client.HTTPResponse(MemorySocket())
        try:
            response.begin()
            if response.status != 200:
                raise WorkerError("native metadata unavailable")
            for name in ("Content-Length","Transfer-Encoding","Content-Encoding"):
                if len(response.headers.get_all(name,[])) > 1:
                    raise WorkerError("native metadata unavailable")
            if (response.headers.get("Content-Encoding", "identity") != "identity"
                    or response.headers.get("Transfer-Encoding", "chunked") != "chunked"
                    or (response.headers.get("Transfer-Encoding") and response.headers.get("Content-Length"))):
                raise WorkerError("native metadata unavailable")
            body = response.read(65537)
            remaining()
            if not 0 < len(body) <= 65536:
                raise WorkerError("native metadata unavailable")
            return parse_native_metadata(body, **config)
        finally:
            response.close()
    except Exception:
        raise WorkerError("native metadata unavailable") from None
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                raise WorkerError("native metadata unavailable") from None


class BootstrapPoll:
    """Metadata-only initialization. This object never constructs a CLI or grant."""
    def __init__(self, coordinates, native_identity, *, clock, wall_clock, started_at, sleep):
        self._coordinates = parse_coordinates(wire.encode(coordinates))
        self._native = wire.detached(native_identity)
        wire.fields(self._native,{"task_arn","task_id","task_definition_arn","image_digest","container_name"})
        self._clock, self._wall, self._sleep = clock, wall_clock, sleep
        self._at = self._last = wire.number(started_at)
        self._last_wall = wire.number(wall_clock())
        self._issued = {}
        self._state = "unbound"

    def _now(self):
        now = wire.number(self._clock())
        if self._state == "held" or not self._last <= now < self._at+30:
            raise WorkerError("bootstrap unavailable")
        self._last = now
        return now

    def _disconnected(self):
        self._state = "held"

    def _response(self, body):
        import hashlib
        if type(body) is not bytes or not 0 < len(body) <= 8192:
            raise WorkerError("bootstrap response rejected")
        value = wire.parse(body)
        wire._common(value)
        if (value["verb"] != "BOOTSTRAP_INFO" or value["connection_generation"] is not None
                or value["invocation_id"] is not None or value["rpc_id"] not in self._issued):
            raise WorkerError("bootstrap response rejected")
        request, sent_at = self._issued[value["rpc_id"]]
        if any(value[k] != request[k] for k in ("attempt_id","request_id")):
            raise WorkerError("bootstrap response rejected")
        data = value["data"]
        wire.fields(data,{"boot_nonce","binding","typed_payload","deadline_at","expires_at"})
        if data["boot_nonce"] != request["data"]["boot_nonce"]:
            raise WorkerError("bootstrap response rejected")
        binding, typed = data["binding"], data["typed_payload"]
        _validate_binding(binding)
        if value["manifest_sha256"] != binding["manifest_sha256"]:
            raise WorkerError("bootstrap response rejected")
        for key in ("request_id","request_spec_sha256","canonical_sha256","spec_sha256","input_sha256","source_sha256"):
            if binding[key] != self._coordinates[key]:
                raise WorkerError("bootstrap response rejected")
        expected_executor = dict(task_arn=self._native["task_arn"],
            task_definition_arn=self._native["task_definition_arn"],launch_token=self._coordinates["launch_token"])
        if binding["executor"] != expected_executor:
            raise WorkerError("bootstrap response rejected")
        wire.fields(typed,{"schema","operation","task_id","request_spec_sha256","timeout"})
        if (type(typed["schema"]) is not int or typed["schema"] != 1
                or typed["operation"] != "nonpricing.context"
                or type(typed["task_id"]) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}",typed["task_id"]) is None
                or type(typed["timeout"]) is not int or not 1 <= typed["timeout"] <= 30
                or typed["request_spec_sha256"] != binding["request_spec_sha256"]
                or hashlib.sha256(wire.encode(typed)).hexdigest() != binding["spec_sha256"]):
            raise WorkerError("bootstrap response rejected")
        deadline, expires = wire.number(data["deadline_at"]), wire.number(data["expires_at"])
        if expires > deadline:
            raise WorkerError("bootstrap response rejected")
        return value, sent_at

    def bind(self, link):
        import uuid
        if self._state != "unbound":
            raise WorkerError("bootstrap already consumed")
        self._state = "bootstrapping"
        link.set_disconnect_callback(self._disconnected)
        try:
            for _ in range(16):
                at = self._now()
                data = {k:v for k,v in self._coordinates.items() if k not in ("attempt_id","request_id")}
                data["boot_nonce"] = uuid.uuid4().hex
                frame = dict(schema=2,attempt_id=self._coordinates["attempt_id"],request_id=self._coordinates["request_id"],
                    connection_generation=None,invocation_id=None,rpc_id=uuid.uuid4().hex,
                    verb="BOOTSTRAP",manifest_sha256=None,data=data)
                self._issued[frame["rpc_id"]] = (frame,at)
                link.send(wire.encode(frame))
                stop = min(at+2,self._at+30)
                while self._now() < stop:
                    body = link.receive_bootstrap(stop)
                    now = self._now()
                    if body is None:
                        break
                    value, sent_at = self._response(body)
                    if value["rpc_id"] != frame["rpc_id"] or now >= sent_at+2:
                        continue
                    wall = wire.number(self._wall())
                    data = value["data"]
                    if (wall < self._last_wall or not wall < data["expires_at"] <= min(data["deadline_at"],wall+2)
                            or not wall < data["deadline_at"] <= wall+120):
                        raise WorkerError("bootstrap metadata expired")
                    self._last_wall = wall
                    link.complete_bootstrap()
                    self._state = "bound"
                    return wire.detached({key:data[key] for key in ("binding","typed_payload","deadline_at")})
                remaining = self._at+30-self._now()
                self._sleep(min(.25,remaining))
            raise WorkerError("bootstrap budget exhausted")
        except Exception:
            self._state = "held"
            try:
                link.abort_bootstrap()
            except Exception:
                pass
            raise WorkerError("bootstrap unavailable") from None


class BootstrapMqttLink(MqttLink):
    """One metadata-only timeout exception; inherited operational receive unchanged."""
    def __init__(self, **kwargs):
        self._bootstrap_open = True
        super().__init__(**kwargs)

    def receive_bootstrap(self, deadline):
        import queue
        deadline = wire.number(deadline)
        last = wire.number(self._clock())
        while True:
            with self._lock:
                if not self._bootstrap_open or self._held or not self._connected:
                    raise WorkerError("bootstrap transport unavailable")
            now = wire.number(self._clock())
            if now < last:
                self._hold()
                raise WorkerError("bootstrap transport unavailable")
            last = now
            if now >= deadline:
                return None
            try:
                body = self._queue.get(timeout=min(deadline-now,.05))
            except queue.Empty:
                continue
            with self._lock:
                if self._held:
                    raise WorkerError("bootstrap transport unavailable")
            # Poll validation sees even a late popped frame, so an unissued RPC
            # cannot disappear merely because the local receive deadline elapsed.
            return body

    def complete_bootstrap(self):
        with self._lock:
            if (not self._bootstrap_open or self._held or not self._connected
                    or not self._queue.empty()):
                self._hold()
                raise WorkerError("bootstrap completion rejected")
            self._bootstrap_open = False

    def abort_bootstrap(self):
        with self._lock:
            self._bootstrap_open = False
        self._hold()
