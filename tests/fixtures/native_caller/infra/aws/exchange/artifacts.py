"""Bounded local original-byte artifacts. Explicit private directory, no network.

This is an application-artifact quota, not a filesystem or arbitrary-process
quota. The caller owns scheduling collect(), worker cleanup and SQLite/WAL
retention. A corrupted or foreign file holds the store for owner reconciliation.
"""
from __future__ import annotations
import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat
import struct
import threading
from .protocol import ExchangeError, INPUT_MAX, RESULT_MAX, detached, encode, number

_HEADER = struct.Struct("!8sddI32s32s")
_MAGIC = b"ITOART01"
_NAME = re.compile(r"([0-9a-f]{64})\.(input|result|summary)")
_LIMITS = {"input": INPUT_MAX, "result": RESULT_MAX, "summary": 4096}


class ArtifactStore:
    def __init__(self, root, *, clock, max_bytes=8*1024*1024, max_files=16):
        self.root = Path(root)
        self._fd = self._lockfd = -1
        self._mutex = threading.RLock()
        self._clock = clock
        self._seen_time = 0
        if (type(max_bytes) is not int or not 1 <= max_bytes <= 8*1024*1024
                or type(max_files) is not int or not 1 <= max_files <= 16
                or not callable(clock)):
            raise ExchangeError("artifact_config")
        self._max_bytes, self._max_files = max_bytes, max_files
        try:
            if not self.root.is_absolute() or self.root.resolve(strict=True) != self.root:
                raise ExchangeError("artifact_path")
            self._fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            s = os.fstat(self._fd)
            if s.st_uid != os.getuid() or s.st_mode & 0o077:
                raise ExchangeError("artifact_permissions")
            self._identity = (s.st_dev, s.st_ino)
            self._lockfd = os.open(".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                                   0o600, dir_fd=self._fd)
            lockstat = os.fstat(self._lockfd)
            if (not stat.S_ISREG(lockstat.st_mode) or lockstat.st_nlink != 1
                    or lockstat.st_uid != os.getuid() or lockstat.st_mode & 0o077):
                raise ExchangeError("artifact_lock")
            fcntl.flock(self._lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._names()
        except Exception:
            self.close()
            raise ExchangeError("artifact_unavailable") from None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        with self._mutex:
            for attr in ("_lockfd", "_fd"):
                fd = getattr(self, attr, -1)
                setattr(self, attr, -1)
                if fd >= 0:
                    os.close(fd)

    def _now(self):
        self._seen_time = max(self._seen_time, number(self._clock()))
        return self._seen_time

    def _check(self):
        if self._fd < 0:
            raise ExchangeError("artifact_closed")
        s = self.root.lstat()
        if (self.root.resolve(strict=True) != self.root or not stat.S_ISDIR(s.st_mode)
                or (s.st_dev, s.st_ino) != self._identity):
            raise ExchangeError("artifact_path_changed")

    def _names(self):
        self._check()
        names = os.listdir(self._fd)
        if any(n != ".lock" and _NAME.fullmatch(n) is None for n in names):
            raise ExchangeError("foreign_artifact")
        names = [n for n in names if n != ".lock"]
        total = 0
        for name in names:
            s = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
                raise ExchangeError("artifact_type")
            total += max(s.st_size, s.st_blocks * 512)
        if len(names) > self._max_files or total > self._max_bytes:
            raise ExchangeError("artifact_quota")
        return names, total

    @staticmethod
    def _key(binding, kind):
        if kind not in _LIMITS:
            raise ExchangeError("artifact_kind")
        raw = encode(binding)
        if len(raw) > 8192:
            raise ExchangeError("artifact_binding")
        digest = hashlib.sha256(raw).digest()
        return digest.hex()+"."+kind, digest

    def _load(self, name, *, verify_payload=True):
        match = _NAME.fullmatch(name)
        if match is None:
            raise ExchangeError("artifact_name")
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self._fd)
        try:
            before = os.fstat(fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_uid != os.getuid() or before.st_mode & 0o077
                    or not _HEADER.size <= before.st_size <= _HEADER.size + _LIMITS[match[2]]):
                raise ExchangeError("artifact_type")
            parts = []
            remaining = before.st_size
            while remaining:
                block = os.read(fd, min(remaining, 65536))
                if not block:
                    raise ExchangeError("artifact_truncated")
                parts.append(block)
                remaining -= len(block)
            raw = b"".join(parts)
            after = os.fstat(fd)
            entry = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_nlink)
            if identity(before) != identity(after) or identity(after) != identity(entry):
                raise ExchangeError("artifact_changed")
            header = _HEADER.unpack(raw[:_HEADER.size])
            magic, created, expires, size, binding_digest, digest = header
            payload = raw[_HEADER.size:]
            if (magic != _MAGIC or binding_digest.hex() != match[1] or not 0 < size <= _LIMITS[match[2]]
                    or not 0 < expires-created <= 86400 or number(created) != created
                    or number(expires) != expires
                    or (verify_payload and (size != len(payload) or hashlib.sha256(payload).digest() != digest))):
                raise ExchangeError("artifact_corrupt")
            return header, payload
        finally:
            os.close(fd)

    def publish(self, binding, kind, data, *, created_at, expires_at):
        with self._mutex:
            try:
                name, binding_digest = self._key(binding, kind)
                created_at, expires_at = number(created_at), number(expires_at)
                now = self._now()
                if (type(data) is not bytes or not 0 < len(data) <= _LIMITS[kind]
                        or not created_at <= now < expires_at <= created_at+86400):
                    raise ExchangeError("artifact_bounds")
                header = (_MAGIC, created_at, expires_at, len(data),
                          binding_digest, hashlib.sha256(data).digest())
                names, total = self._names()
                if name in names:
                    old, payload = self._load(name)
                    if old != header or payload != data:
                        raise ExchangeError("artifact_conflict")
                    return False
                size = _HEADER.size + len(data)
                block = os.fstatvfs(self._fd).f_frsize or 4096
                reserved = ((size + block - 1) // block) * block
                if len(names) >= self._max_files or total+reserved > self._max_bytes:
                    raise ExchangeError("artifact_quota")
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=self._fd)
                try:
                    value = memoryview(_HEADER.pack(*header)+data)
                    while value:
                        written = os.write(fd, value)
                        if written <= 0:
                            raise ExchangeError("artifact_write")
                        value = value[written:]
                    os.fsync(fd)
                except Exception:
                    os.unlink(name, dir_fd=self._fd)
                    raise
                finally:
                    os.close(fd)
                os.fsync(self._fd)
                try:
                    self._names()
                except Exception:
                    os.unlink(name, dir_fd=self._fd)
                    os.fsync(self._fd)
                    raise
                return True
            except ExchangeError:
                raise
            except Exception:
                raise ExchangeError("artifact_io_unknown") from None

    def read(self, binding, kind, *, max_bytes):
        with self._mutex:
            try:
                if type(max_bytes) is not int or not 0 < max_bytes <= _LIMITS.get(kind, 0):
                    raise ExchangeError("artifact_read_bound")
                name, _ = self._key(binding, kind)
                self._names()
                header, data = self._load(name)
                if self._now() >= header[2]:
                    os.unlink(name, dir_fd=self._fd)
                    os.fsync(self._fd)
                    raise ExchangeError("artifact_expired")
                if len(data) > max_bytes:
                    raise ExchangeError("artifact_read_bound")
                return data
            except ExchangeError:
                raise
            except FileNotFoundError:
                raise ExchangeError("artifact_missing") from None
            except Exception:
                raise ExchangeError("artifact_read_unavailable") from None

    def read_result(self, binding, *, max_bytes):
        return {"binding": detached(binding), "stdout": self.read(binding, "result", max_bytes=max_bytes)}

    def collect(self):
        with self._mutex:
            try:
                names, _ = self._names()
                now = self._now()
                removed = 0
                for name in names:
                    header, _ = self._load(name, verify_payload=False)
                    if now >= header[2]:
                        os.unlink(name, dir_fd=self._fd)
                        removed += 1
                os.fsync(self._fd)
                names, total = self._names()
                return {"removed": removed, "remaining_files": len(names), "retained_bytes": total}
            except ExchangeError:
                raise
            except Exception:
                raise ExchangeError("artifact_collection_unknown") from None
