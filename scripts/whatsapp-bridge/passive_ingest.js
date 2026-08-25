import path from 'node:path';
import { createHash } from 'node:crypto';
import {
  closeSync,
  constants,
  fstatSync,
  fsyncSync,
  openSync,
  writeFileSync,
} from 'node:fs';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

import { getMessageContent } from './bridge_helpers.js';

const MAX_TEXT_CHARS = 16 * 1024;
const DEFAULT_MAX_QUEUE = 256;

function normalizeJid(value) {
  return String(value || '').trim().replace(/:.*@/, '@');
}

function conversationKind(chatId) {
  if (!chatId.includes('@')) return null;
  if (chatId.endsWith('@s.whatsapp.net') || chatId.endsWith('@lid')) return 'dm';
  if (chatId.endsWith('@g.us')) return 'group';
  if (chatId === 'status@broadcast') return 'status';
  if (chatId.endsWith('@broadcast')) return 'broadcast';
  if (chatId.endsWith('@newsletter')) return 'newsletter';
  return 'other';
}

function messageProjection(msg) {
  const content = getMessageContent(msg);
  if (content.conversation) return { text: content.conversation, mediaType: null };
  if (content.extendedTextMessage?.text) {
    return { text: content.extendedTextMessage.text, mediaType: null };
  }
  const media = [
    ['imageMessage', 'image'],
    ['videoMessage', 'video'],
    ['audioMessage', 'audio'],
    ['pttMessage', 'audio'],
    ['documentMessage', 'document'],
    ['stickerMessage', 'sticker'],
  ];
  for (const [key, mediaType] of media) {
    if (!content[key]) continue;
    const caption = String(content[key].caption || '').trim();
    return { text: caption || `[${mediaType} received]`, mediaType };
  }
  const nativeKey = Object.keys(content)[0];
  if (!nativeKey) return null;
  return {
    text: null,
    mediaType: nativeKey.replace(/Message$/, '') || 'unknown',
  };
}

function messageContext(msg) {
  const content = getMessageContent(msg);
  for (const value of Object.values(content)) {
    if (value && typeof value === 'object' && value.contextInfo) return value.contextInfo;
  }
  return {};
}

export function passiveIngestRecord({ msg, type, ownerJids = [] }) {
  const chatId = String(msg?.key?.remoteJid || '').trim();
  const messageId = String(msg?.key?.id || '').trim();
  const kind = conversationKind(chatId);
  if (!msg?.message || !chatId || !messageId || kind === null) return null;
  const projection = messageProjection(msg);
  if (!projection) return null;
  const fromMe = msg.key.fromMe === true;
  const rawTimestamp = msg.messageTimestamp;
  const timestamp = Number(
    typeof rawTimestamp === 'object' && rawTimestamp !== null && 'low' in rawTimestamp
      ? rawTimestamp.low
      : rawTimestamp,
  );
  if (!Number.isSafeInteger(timestamp) || timestamp <= 0) return null;
  const senderId = fromMe ? 'self' : String(msg.key.participant || chatId).trim();
  const record = {
    schema: 1,
    providerMessageId: messageId,
    chatId,
    senderId,
    senderName: fromMe ? 'self' : String(msg.pushName || senderId).trim(),
    timestamp,
    direction: fromMe ? 'outbound' : 'inbound',
    kind,
    text: projection.text === null ? null : String(projection.text).slice(0, MAX_TEXT_CHARS),
    mediaType: projection.mediaType,
    upsertType: type,
  };
  if (kind === 'group') {
    const context = messageContext(msg);
    const mentionedIds = Array.isArray(context.mentionedJid)
      ? context.mentionedJid.map(normalizeJid).filter(Boolean)
      : [];
    const quotedParticipant = normalizeJid(context.participant);
    const owners = new Set(ownerJids.map(normalizeJid).filter(Boolean));
    record.groupAddressing = {
      mentionedIds,
      quotedParticipant: quotedParticipant || null,
      quotedMessageId: String(context.stanzaId || '').trim() || null,
      ownerAddressed: mentionedIds.some((jid) => owners.has(jid))
        || (quotedParticipant !== '' && owners.has(quotedParticipant)),
    };
  }
  return record;
}

export function spoolDestinationFingerprint(spoolPath) {
  const normalized = path.normalize(String(spoolPath || ''));
  return createHash('sha256').update(normalized).digest('hex').slice(0, 16);
}

export function validateSpoolInfo(info) {
  if (!info?.isFile?.() || (info.mode & 0o777) !== 0o600) {
    throw new Error('WhatsApp passive-ingest spool must be a regular owner-only file');
  }
  if (typeof process.getuid === 'function' && info.uid !== process.getuid()) {
    throw new Error('WhatsApp passive-ingest spool must be owned by the gateway user');
  }
}

function accessPrivateSpool(spoolPath, line = null) {
  if (!path.isAbsolute(spoolPath) || path.normalize(spoolPath) !== spoolPath) {
    throw new Error('WhatsApp passive-ingest spool path must be absolute and normalized');
  }
  const noFollow = constants.O_NOFOLLOW;
  const nonBlock = constants.O_NONBLOCK;
  if (!Number.isInteger(noFollow) || noFollow <= 0 || !Number.isInteger(nonBlock) || nonBlock <= 0) {
    throw new Error('WhatsApp passive-ingest requires non-blocking no-follow file access');
  }
  let fd;
  try {
    fd = openSync(
      spoolPath,
      constants.O_WRONLY | constants.O_APPEND | constants.O_CREAT | noFollow | nonBlock,
      0o600,
    );
    validateSpoolInfo(fstatSync(fd));
    if (line !== null) writeFileSync(fd, line, { encoding: 'utf8' });
    fsyncSync(fd);
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function errorText(error) {
  return error instanceof Error ? error.message : String(error);
}

if (!isMainThread && workerData?.hermesPassiveIngestWorker === true) {
  const spoolPath = workerData.spoolPath;
  try {
    accessPrivateSpool(spoolPath);
    parentPort.postMessage({ type: 'ready' });
  } catch (error) {
    parentPort.postMessage({ type: 'preflight-error', error: errorText(error) });
  }
  parentPort.on('message', ({ type, id, line }) => {
    if (type === 'close') {
      parentPort.postMessage({ type: 'closed' });
      parentPort.close();
      return;
    }
    if (type !== 'append') return;
    try {
      accessPrivateSpool(spoolPath, line);
      parentPort.postMessage({ type: 'result', id, ok: true });
    } catch (error) {
      parentPort.postMessage({ type: 'result', id, ok: false, error: errorText(error) });
    }
  });
}

export function createPassiveIngestSpool({
  enabled = false,
  spoolPath = '',
  maxQueue = DEFAULT_MAX_QUEUE,
} = {}) {
  const active = Boolean(enabled);
  const capacity = Number.isSafeInteger(maxQueue) && maxQueue > 0 ? maxQueue : DEFAULT_MAX_QUEUE;
  let healthy = !active;
  let lastError = active ? 'WhatsApp passive-ingest spool preflight pending' : null;
  let pending = 0;
  let nextId = 1;
  let lastFailureId = 0;
  let worker = null;
  let closeResolve = null;

  if (active) {
    worker = new Worker(new URL(import.meta.url), {
      workerData: { hermesPassiveIngestWorker: true, spoolPath },
    });
    worker.on('message', (message) => {
      if (message.type === 'ready') {
        if (lastFailureId === 0) {
          healthy = true;
          lastError = null;
        }
      } else if (message.type === 'preflight-error') {
        healthy = false;
        lastError = message.error;
      } else if (message.type === 'result') {
        pending = Math.max(0, pending - 1);
        if (!message.ok) {
          lastFailureId = Math.max(lastFailureId, message.id);
          healthy = false;
          lastError = message.error;
        } else if (message.id > lastFailureId) {
          healthy = true;
          lastError = null;
        }
      } else if (message.type === 'closed' && closeResolve) {
        closeResolve();
        closeResolve = null;
      }
    });
    worker.on('error', (error) => {
      healthy = false;
      lastError = errorText(error);
    });
    worker.on('exit', (code) => {
      if (code !== 0 && closeResolve === null) {
        healthy = false;
        lastError = `WhatsApp passive-ingest writer exited with code ${code}`;
      }
    });
  }

  return {
    enabled: active,
    destinationFingerprint: spoolDestinationFingerprint(spoolPath),
    get healthy() { return healthy; },
    get lastError() { return lastError; },
    get queueLength() { return pending; },
    append({ msg, type, ownerJids = [] }) {
      if (!active) return false;
      const record = passiveIngestRecord({ msg, type, ownerJids });
      if (!record) return false;
      if (pending >= capacity) {
        lastFailureId = nextId++;
        healthy = false;
        lastError = `WhatsApp passive-ingest queue capacity ${capacity} exceeded`;
        return false;
      }
      pending += 1;
      worker.postMessage({
        type: 'append',
        id: nextId++,
        line: `${JSON.stringify(record)}\n`,
      });
      return true;
    },
    async close() {
      if (!worker) return;
      await new Promise((resolve) => {
        closeResolve = resolve;
        worker.postMessage({ type: 'close' });
      });
      await worker.terminate();
      worker = null;
    },
  };
}
