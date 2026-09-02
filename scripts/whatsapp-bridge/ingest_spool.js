import {
  closeSync,
  constants,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readSync,
  readdirSync,
  writeSync,
} from 'node:fs';
import { randomBytes } from 'node:crypto';
import path from 'node:path';

const MAX_TEXT_CHARS = 2048;
const MAX_ID_CHARS = 512;
const MAX_OWNER_IDS = 16;
const DEFAULT_SEGMENT_BYTES = 8 * 1024 * 1024;
const FAILURE_MARKER_BYTES = 256;

export class SpoolCeilingError extends Error {}
export class InvalidIngestEvent extends Error {}

function unwrappedMessage(msg) {
  let value = msg?.message || {};
  const wrappers = [
    'ephemeralMessage',
    'viewOnceMessage',
    'viewOnceMessageV2',
    'documentWithCaptionMessage',
  ];
  for (let depth = 0; depth < 8; depth += 1) {
    const wrapper = wrappers.find((key) => value?.[key]?.message);
    if (!wrapper) break;
    value = value[wrapper].message;
  }
  return value || {};
}

function timestampSeconds(value) {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value > 0) return value;
  if (typeof value === 'bigint' && value > 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value);
  }
  if (value && Number.isInteger(value.low) && Number.isInteger(value.high)) {
    const converted = (BigInt(value.high >>> 0) << 32n) | BigInt(value.low >>> 0);
    if (converted > 0n && converted <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(converted);
  }
  throw new InvalidIngestEvent('invalid message timestamp');
}

function bounded(value) {
  if (value === undefined || value === null || value === '') return '';
  if (typeof value !== 'string') throw new InvalidIngestEvent('invalid message text');
  return value.slice(0, MAX_TEXT_CHARS).replaceAll('\u0000', '');
}

function boundedId(value, label) {
  if (typeof value !== 'string' || value.length > MAX_ID_CHARS) {
    throw new InvalidIngestEvent(`invalid ${label}`);
  }
  const result = value.trim();
  if (!result || result.includes('\u0000')) throw new InvalidIngestEvent(`invalid ${label}`);
  return result;
}

function normalizedJid(value) {
  if (value === undefined || value === null || value === '') return '';
  return boundedId(value, 'jid').replace(/:\d+@/, '@');
}

function jidKind(chatId) {
  if (/@g\.us$/.test(chatId)) return 'group';
  if (/@(?:s\.whatsapp\.net|lid)$/.test(chatId)) return 'dm';
  return null;
}

function normalizedText(message) {
  if (message.conversation) return bounded(message.conversation);
  if (message.extendedTextMessage?.text) return bounded(message.extendedTextMessage.text);
  if (message.imageMessage) return bounded(message.imageMessage.caption) || '[image received]';
  if (message.videoMessage) return bounded(message.videoMessage.caption) || '[video received]';
  if (message.audioMessage || message.pttMessage) return '[audio received]';
  if (message.documentMessage) return bounded(message.documentMessage.caption) || '[document received]';
  if (message.stickerMessage) return '[sticker received]';
  if (message.contactMessage || message.contactsArrayMessage) return '[contact received]';
  if (message.locationMessage || message.liveLocationMessage) return '[location received]';
  if (message.pollCreationMessage || message.pollCreationMessageV3) return '[poll received]';
  if (message.reactionMessage) return bounded(message.reactionMessage.text) || '[reaction received]';
  return '[unsupported message received]';
}

function contextInfo(message) {
  if (!message || typeof message !== 'object' || Array.isArray(message)) return {};
  const candidates = [
    message.extendedTextMessage,
    message.imageMessage,
    message.videoMessage,
    message.audioMessage,
    message.documentMessage,
    message.stickerMessage,
    message.contactMessage,
    message.contactsArrayMessage,
    message.locationMessage,
    message.liveLocationMessage,
    message.pollCreationMessage,
    message.pollCreationMessageV3,
    message.reactionMessage,
  ];
  for (const value of candidates) {
    if (value && typeof value === 'object' && value.contextInfo !== undefined) {
      if (!value.contextInfo || typeof value.contextInfo !== 'object' || Array.isArray(value.contextInfo)) {
        throw new InvalidIngestEvent('invalid context info');
      }
      return value.contextInfo;
    }
  }
  return {};
}

export function normalizeEvent(msg, { ownerId = '', ownerIds = [] } = {}) {
  if (!Array.isArray(ownerIds) || ownerIds.length > MAX_OWNER_IDS) {
    throw new InvalidIngestEvent('invalid owner id set');
  }
  const key = msg?.key || {};
  const chatId = boundedId(normalizedJid(key.remoteJid), 'chat id');
  const conversationType = jidKind(chatId);
  if (conversationType === null) return null;
  const providerMessageId = boundedId(key.id, 'provider message id');
  const fromOwner = key.fromMe === true;
  const owners = new Set();
  let firstOwner = '';
  const addOwner = (value) => {
    const normalized = normalizedJid(value);
    if (!normalized) return;
    if (!firstOwner) firstOwner = normalized;
    owners.add(normalized);
  };
  addOwner(ownerId);
  for (let index = 0; index < ownerIds.length; index += 1) {
    addOwner(ownerIds[index]);
  }
  const senderId = boundedId(
    normalizedJid(key.participant || (fromOwner ? firstOwner : chatId)),
    'sender id',
  );
  const message = unwrappedMessage(msg);
  const context = contextInfo(message);
  const mentionedJid = context.mentionedJid ?? [];
  if (!Array.isArray(mentionedJid) || mentionedJid.length > 64) {
    throw new InvalidIngestEvent('invalid mentioned jid list');
  }
  const addressed = new Set();
  for (let index = 0; index < mentionedJid.length; index += 1) {
    const normalized = normalizedJid(mentionedJid[index]);
    if (normalized) addressed.add(normalized);
  }
  const participant = normalizedJid(context.participant);
  if (participant) addressed.add(participant);
  const ownerAddressed = conversationType === 'dm'
    ? !fromOwner
    : !fromOwner && [...addressed].some((jid) => owners.has(jid));
  return {
    version: 2,
    provider_message_id: providerMessageId,
    chat_id: chatId,
    sender_id: senderId,
    timestamp: timestampSeconds(msg.messageTimestamp),
    direction: fromOwner ? 'owner_outbound' : 'external_inbound',
    conversation_type: conversationType,
    owner_addressed: ownerAddressed,
    text: normalizedText(message),
  };
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function writeFully(fd, payload) {
  let written = 0;
  while (written < payload.length) {
    const count = writeSync(fd, payload, written, payload.length - written);
    if (count <= 0) throw new Error('spool write made no progress');
    written += count;
  }
}

function ensurePrivateParent(spoolPath) {
  if (!path.isAbsolute(spoolPath) || path.normalize(spoolPath) !== spoolPath) {
    throw new Error('spool path must be absolute and normalized');
  }
  if (!Number.isInteger(constants.O_NOFOLLOW) || constants.O_NOFOLLOW <= 0) {
    throw new Error('secure no-follow opens are unavailable');
  }
  const parent = path.dirname(spoolPath);
  const parentParent = path.dirname(parent);
  let component = path.parse(parentParent).root;
  for (const part of parentParent.slice(component.length).split(path.sep).filter(Boolean)) {
    component = path.join(component, part);
    const info = lstatSync(component);
    if (!info.isDirectory() || info.isSymbolicLink()) {
      throw new Error('spool path contains a non-directory or symlink');
    }
    const mode = info.mode & 0o777;
    const unsafe = (mode & 0o022) !== 0 && !(info.uid === 0 && (info.mode & 0o1000) !== 0);
    if (unsafe) throw new Error('spool path contains a writable ancestor');
  }
  try {
    const existing = lstatSync(parent);
    if (!existing.isDirectory() || existing.isSymbolicLink()) {
      throw new Error('spool parent must be a real directory');
    }
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    mkdirSync(parent, { mode: 0o700 });
  }
  const parentInfo = lstatSync(parent);
  if (
    !parentInfo.isDirectory() || parentInfo.isSymbolicLink()
    || parentInfo.uid !== process.geteuid() || (parentInfo.mode & 0o777) !== 0o700
  ) {
    throw new Error('spool parent must be owner-private');
  }
  const parentFd = openSync(
    parent,
    constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
  );
  const descriptorInfo = fstatSync(parentFd);
  if (descriptorInfo.dev !== parentInfo.dev || descriptorInfo.ino !== parentInfo.ino) {
    closeSync(parentFd);
    throw new Error('spool parent changed while it was opened');
  }
  return { parent, parentInfo, parentFd };
}

function assertParentStable(parent, parentInfo) {
  const current = lstatSync(parent);
  if (
    !current.isDirectory() || current.isSymbolicLink()
    || current.dev !== parentInfo.dev || current.ino !== parentInfo.ino
  ) {
    throw new Error('spool parent changed after startup');
  }
}

function segmentPattern(spoolPath) {
  const extension = path.extname(spoolPath);
  const stem = path.basename(spoolPath, extension);
  return new RegExp(`^${escapeRegExp(stem)}(?:\\.([0-9]{13})\\.([0-9a-f]{16}))?${escapeRegExp(extension)}$`);
}

function listSegments(spoolPath, parent, parentInfo, maxSegmentBytes) {
  assertParentStable(parent, parentInfo);
  const pattern = segmentPattern(spoolPath);
  const segments = [];
  for (const name of readdirSync(parent)) {
    const match = pattern.exec(name);
    if (!match) continue;
    const fullPath = path.join(parent, name);
    const info = lstatSync(fullPath);
    if (
      !info.isFile() || info.isSymbolicLink() || info.uid !== process.geteuid()
      || (info.mode & 0o777) !== 0o600 || info.size > maxSegmentBytes
    ) {
      throw new Error('spool segment is not a bounded owner-only regular file');
    }
    segments.push({
      name,
      fullPath,
      size: info.size,
      dev: info.dev,
      ino: info.ino,
      generation: match[1] ? Number(match[1]) : 0,
    });
  }
  return segments.sort((left, right) => (
    left.generation - right.generation || left.name.localeCompare(right.name)
  ));
}

function openSegment(fullPath, parent, parentInfo, { create = false } = {}) {
  assertParentStable(parent, parentInfo);
  const flags = constants.O_WRONLY | constants.O_APPEND | constants.O_NOFOLLOW
    | (create ? constants.O_CREAT | constants.O_EXCL : 0);
  const fd = openSync(fullPath, flags, 0o600);
  try {
    const descriptor = fstatSync(fd);
    const pathname = lstatSync(fullPath);
    assertParentStable(parent, parentInfo);
    if (
      !descriptor.isFile() || pathname.isSymbolicLink()
      || pathname.dev !== descriptor.dev || pathname.ino !== descriptor.ino
      || descriptor.uid !== process.geteuid() || (descriptor.mode & 0o777) !== 0o600
    ) {
      throw new Error('spool segment changed while it was opened');
    }
    return fd;
  } catch (error) {
    closeSync(fd);
    throw error;
  }
}

function markerPayload(status, code, at) {
  const raw = Buffer.from(`${JSON.stringify({ version: 2, status, code, at })}\n`, 'utf8');
  if (raw.length > FAILURE_MARKER_BYTES) throw new Error('spool failure marker exceeds reserve');
  return Buffer.concat([raw, Buffer.alloc(FAILURE_MARKER_BYTES - raw.length, 0x20)]);
}

function writeMarker(fd, fullPath, parent, parentInfo, status, code, at) {
  assertParentStable(parent, parentInfo);
  const descriptor = fstatSync(fd);
  const pathname = lstatSync(fullPath);
  if (
    !descriptor.isFile() || pathname.isSymbolicLink()
    || pathname.dev !== descriptor.dev || pathname.ino !== descriptor.ino
    || descriptor.uid !== process.geteuid() || (descriptor.mode & 0o777) !== 0o600
    || descriptor.size !== FAILURE_MARKER_BYTES
  ) throw new Error('spool failure marker changed while open');
  const payload = markerPayload(status, code, at);
  let written = 0;
  while (written < payload.length) {
    const count = writeSync(fd, payload, written, payload.length - written, written);
    if (count <= 0) throw new Error('spool failure marker write made no progress');
    written += count;
  }
  fsyncSync(fd);
}

function openFailureMarker(fullPath, parent, parentInfo, parentFd) {
  assertParentStable(parent, parentInfo);
  let markerFd;
  let created = false;
  try {
    markerFd = openSync(
      fullPath,
      constants.O_RDWR | constants.O_NOFOLLOW | (constants.O_NONBLOCK || 0),
    );
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    markerFd = openSync(
      fullPath,
      constants.O_RDWR | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    created = true;
  }
  try {
    if (created) {
      writeFully(markerFd, markerPayload('clean', null, null));
      fsyncSync(markerFd);
      fsyncSync(parentFd);
    }
    const descriptor = fstatSync(markerFd);
    const pathname = lstatSync(fullPath);
    if (
      !descriptor.isFile() || pathname.isSymbolicLink()
      || pathname.dev !== descriptor.dev || pathname.ino !== descriptor.ino
      || descriptor.uid !== process.geteuid() || (descriptor.mode & 0o777) !== 0o600
      || descriptor.size !== FAILURE_MARKER_BYTES
    ) throw new Error('spool failure marker is unsafe');
    const bytes = Buffer.alloc(FAILURE_MARKER_BYTES + 1);
    const count = readSync(markerFd, bytes, 0, bytes.length, 0);
    if (count !== FAILURE_MARKER_BYTES) throw new Error('spool failure marker size changed');
    const parsed = JSON.parse(bytes.subarray(0, count).toString('utf8'));
    if (
      parsed?.version !== 2 || !['clean', 'armed', 'failed'].includes(parsed.status)
      || (parsed.code !== null && (typeof parsed.code !== 'string' || parsed.code.length > 64))
      || (parsed.at !== null && !Number.isSafeInteger(parsed.at))
    ) throw new Error('spool failure marker is invalid');
    return { fd: markerFd, state: parsed };
  } catch (error) {
    closeSync(markerFd);
    throw error;
  }
}

export function createDurableSpool({
  spoolPath,
  maxSpoolBytes,
  maxRecordBytes,
  maxSegmentBytes = Math.min(DEFAULT_SEGMENT_BYTES, maxSpoolBytes),
  maxBatchRecords = 1024,
  maxBatchBytes = Math.min(8 * 1024 * 1024, maxSpoolBytes),
  syncSegment = fsyncSync,
}) {
  if (!Number.isSafeInteger(maxSpoolBytes) || maxSpoolBytes <= 0) {
    throw new TypeError('maxSpoolBytes must be a positive integer');
  }
  if (!Number.isSafeInteger(maxRecordBytes) || maxRecordBytes <= 0) {
    throw new TypeError('maxRecordBytes must be a positive integer');
  }
  if (
    !Number.isSafeInteger(maxSegmentBytes) || maxSegmentBytes <= 0
    || maxSegmentBytes > maxSpoolBytes
  ) {
    throw new TypeError('maxSegmentBytes must be positive and no larger than maxSpoolBytes');
  }
  if (!Number.isSafeInteger(maxBatchRecords) || maxBatchRecords <= 0) {
    throw new TypeError('maxBatchRecords must be a positive integer');
  }
  if (
    !Number.isSafeInteger(maxBatchBytes) || maxBatchBytes <= 0
    || maxBatchBytes > maxSpoolBytes
  ) {
    throw new TypeError('maxBatchBytes must be positive and no larger than maxSpoolBytes');
  }
  if (typeof syncSegment !== 'function') throw new TypeError('syncSegment must be a function');

  const { parent, parentInfo, parentFd } = ensurePrivateParent(spoolPath);
  let segments = listSegments(spoolPath, parent, parentInfo, maxSegmentBytes);
  if (segments.length === 0) {
    const fd = openSegment(spoolPath, parent, parentInfo, { create: true });
    fsyncSync(parentFd);
    closeSync(fd);
    segments = listSegments(spoolPath, parent, parentInfo, maxSegmentBytes);
  }
  let active = segments.at(-1);
  let lastGeneration = active.generation;
  let fd = openSegment(active.fullPath, parent, parentInfo);
  let closed = false;
  const failurePath = `${spoolPath}.failure.json`;
  const marker = openFailureMarker(failurePath, parent, parentInfo, parentFd);
  const existingFailure = marker.state.status === 'clean' ? null : marker.state;
  if (existingFailure === null) {
    writeMarker(marker.fd, failurePath, parent, parentInfo, 'armed', null, Date.now());
  }
  const state = {
    lastAppendAt: null,
    lastErrorAt: existingFailure?.at ?? null,
    lastErrorCode: existingFailure ? (existingFailure.code || 'unclean_exit') : null,
    failureLatched: existingFailure !== null,
  };

  let segmentDirty = false;

  function rotate(payloadLength) {
    segments = listSegments(spoolPath, parent, parentInfo, maxSegmentBytes);
    const total = segments.reduce((sum, segment) => sum + segment.size, 0);
    if (total + payloadLength > maxSpoolBytes) {
      throw new SpoolCeilingError('WhatsApp spool reached total hard ceiling');
    }
    const extension = path.extname(spoolPath);
    const stem = path.basename(spoolPath, extension);
    lastGeneration = Math.max(Date.now(), lastGeneration + 1);
    const name = `${stem}.${String(lastGeneration).padStart(13, '0')}.${randomBytes(8).toString('hex')}${extension}`;
    const fullPath = path.join(parent, name);
    const nextFd = openSegment(fullPath, parent, parentInfo, { create: true });
    fsyncSync(parentFd);
    if (segmentDirty) {
      syncSegment(fd);
      segmentDirty = false;
    }
    closeSync(fd);
    fd = nextFd;
    active = { name, fullPath, size: 0 };
  }

  function latchFailure(error) {
    state.lastErrorAt = Date.now();
    state.lastErrorCode = error instanceof SpoolCeilingError
      ? 'ceiling'
      : error instanceof InvalidIngestEvent ? 'invalid_event' : 'io';
    state.failureLatched = true;
    try {
      writeMarker(
        marker.fd, failurePath, parent, parentInfo,
        'failed', state.lastErrorCode, state.lastErrorAt,
      );
    } catch {
      // The preallocated marker remains in its durable armed state. A
      // restart treats both armed and failed as a known-loss latch.
    }
  }

  function appendBatch(messages, options = {}) {
    if (closed) throw new Error('spool is closed');
    try {
      if (!Array.isArray(messages)) throw new TypeError('messages must be an array');
      if (messages.length > maxBatchRecords) {
        throw new SpoolCeilingError('WhatsApp provider batch exceeds hard record ceiling');
      }
      const captured = [];
      const payloads = [];
      let batchBytes = 0;
      for (const msg of messages) {
        const record = normalizeEvent(msg, options);
        if (record === null) {
          captured.push(false);
          continue;
        }
        const payload = Buffer.from(`${JSON.stringify(record)}\n`, 'utf8');
        if (payload.length > maxRecordBytes || payload.length > maxSegmentBytes) {
          throw new SpoolCeilingError('normalized WhatsApp record exceeds hard ceiling');
        }
        batchBytes += payload.length;
        if (batchBytes > maxBatchBytes) {
          throw new SpoolCeilingError('WhatsApp provider batch exceeds hard byte ceiling');
        }
        captured.push(true);
        payloads.push(payload);
      }
      const retained = listSegments(
        spoolPath,
        parent,
        parentInfo,
        maxSegmentBytes,
      ).reduce((sum, segment) => sum + segment.size, 0);
      if (retained + batchBytes > maxSpoolBytes) {
        throw new SpoolCeilingError('WhatsApp spool reached total hard ceiling');
      }
      for (const payload of payloads) {
        const info = fstatSync(fd);
        if (!info.isFile() || info.uid !== process.geteuid() || (info.mode & 0o777) !== 0o600) {
          throw new Error('spool descriptor is not an owner-only regular file');
        }
        if (info.size + payload.length > maxSegmentBytes) rotate(payload.length);
        writeFully(fd, payload);
        segmentDirty = true;
      }
      if (segmentDirty) {
        syncSegment(fd);
        segmentDirty = false;
      }
      if (payloads.length > 0) state.lastAppendAt = Date.now();
      return captured;
    } catch (error) {
      latchFailure(error);
      throw error;
    }
  }

  return Object.freeze({
    append(msg, options = {}) {
      return appendBatch([msg], options)[0];
    },
    appendBatch,
    health() {
      const current = listSegments(spoolPath, parent, parentInfo, maxSegmentBytes);
      const totalBytes = current.reduce((sum, segment) => sum + segment.size, 0);
      return {
        healthy: !state.failureLatched,
        failureLatched: state.failureLatched,
        lastAppendAt: state.lastAppendAt,
        lastErrorAt: state.lastErrorAt,
        lastErrorCode: state.lastErrorCode,
        segmentCount: current.length,
        totalBytes,
        maxSpoolBytes,
        activeSegment: active.name,
      };
    },
    close() {
      if (!closed) {
        if (!state.failureLatched) {
          writeMarker(marker.fd, failurePath, parent, parentInfo, 'clean', null, null);
        }
        closeSync(marker.fd);
        closeSync(fd);
        closeSync(parentFd);
        closed = true;
      }
    },
  });
}
