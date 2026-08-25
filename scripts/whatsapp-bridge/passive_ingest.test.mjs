import { strict as assert } from 'node:assert';
import { chmodSync, mkdtempSync, readFileSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import * as passive from './passive_ingest.js';

assert.equal(typeof passive.createPassiveIngestSpool, 'function');

async function waitFor(predicate, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('timed out waiting for passive ingest');
    await delay(10);
  }
}

const root = mkdtempSync(path.join(tmpdir(), 'hermes-wa-ingest-'));
const spoolPath = path.join(root, 'whatsapp.ndjson');
const spool = passive.createPassiveIngestSpool({
  enabled: true,
  spoolPath,
});
assert.equal(spool.enabled, true);
assert.equal(spool.healthy, false);
await waitFor(() => spool.healthy);

let mediaDownloads = 0;
const appended = spool.append({
  type: 'notify',
  msg: {
    key: {
      id: 'provider-message-1',
      remoteJid: '15551234567@s.whatsapp.net',
      participant: '15550001111@s.whatsapp.net',
      fromMe: false,
    },
    pushName: 'External Sender',
    messageTimestamp: 1787063000,
    message: { conversation: 'Can you send the current requirements?' },
  },
  downloadMedia: () => { mediaDownloads += 1; },
});

assert.equal(appended, true);
assert.equal(mediaDownloads, 0);
await waitFor(() => spool.queueLength === 0);
assert.equal(statSync(spoolPath).mode & 0o777, 0o600);
const records = readFileSync(spoolPath, 'utf8').trim().split('\n').map(JSON.parse);
assert.deepEqual(records, [{
  schema: 1,
  providerMessageId: 'provider-message-1',
  chatId: '15551234567@s.whatsapp.net',
  senderId: '15550001111@s.whatsapp.net',
  senderName: 'External Sender',
  timestamp: 1787063000,
  direction: 'inbound',
  kind: 'dm',
  text: 'Can you send the current requirements?',
  mediaType: null,
  upsertType: 'notify',
}]);
console.log('  ✓ external DM is durably spooled without media or outbound side effects');

const disabledPath = path.join(root, 'disabled.ndjson');
const disabled = passive.createPassiveIngestSpool({
  enabled: false,
  spoolPath: disabledPath,
});
assert.equal(disabled.append({
  type: 'notify',
  msg: {
    key: { id: 'disabled-1', remoteJid: '15551234567@s.whatsapp.net', fromMe: false },
    messageTimestamp: 1787063001,
    message: { conversation: 'not persisted' },
  },
}), false);
assert.throws(() => statSync(disabledPath));

assert.equal(spool.append({
  type: 'notify',
  msg: {
    key: { id: 'status-1', remoteJid: 'status@broadcast', fromMe: false },
    messageTimestamp: 1787063002,
    message: { conversation: 'status update' },
  },
}), true);
await waitFor(() => spool.queueLength === 0);

for (const [conversationJid, expectedKind] of [
  ['15551234567@s.whatsapp.net', 'dm'],
  ['987654321@lid', 'dm'],
  ['120363001234567890@g.us', 'group'],
  ['status@broadcast', 'status'],
  ['updates@broadcast', 'broadcast'],
  ['channel@newsletter', 'newsletter'],
  ['device@c.us', 'other'],
]) {
  const record = passive.passiveIngestRecord({
    type: 'notify',
    msg: {
      key: { id: `accept-${conversationJid}`, remoteJid: conversationJid, fromMe: false },
      messageTimestamp: 1787063002,
      message: { conversation: 'supported protocol conversation' },
    },
  });
  assert.equal(record.kind, expectedKind, conversationJid);
}

assert.equal(passive.passiveIngestRecord({
  type: 'notify',
  msg: {
    key: { id: 'malformed-jid', remoteJid: 'missing-domain', fromMe: false },
    messageTimestamp: 1787063002,
    message: { conversation: 'not a protocol conversation JID' },
  },
}), null);

const ownerJid = '15550000000@s.whatsapp.net';
const addressedGroup = passive.passiveIngestRecord({
  type: 'notify',
  ownerJids: [ownerJid],
  msg: {
    key: {
      id: 'addressed-group',
      remoteJid: '120363001234567890@g.us',
      participant: '15559999999@s.whatsapp.net',
      fromMe: false,
    },
    messageTimestamp: 1787063002,
    message: {
      extendedTextMessage: {
        text: 'Owner, please review',
        contextInfo: {
          mentionedJid: [ownerJid],
          stanzaId: 'owner-message-1',
          participant: ownerJid,
        },
      },
    },
  },
});
assert.deepEqual(addressedGroup.groupAddressing, {
  mentionedIds: [ownerJid],
  quotedParticipant: ownerJid,
  quotedMessageId: 'owner-message-1',
  ownerAddressed: true,
});

if (typeof process.getuid === 'function') {
  assert.throws(() => passive.validateSpoolInfo({
    isFile: () => true,
    mode: 0o100600,
    uid: process.getuid() + 1,
  }), /owned by the gateway user/);
}

assert.equal(spool.append({
  type: 'append',
  msg: {
    key: {
      id: 'provider-message-2',
      remoteJid: '120363001234567890@g.us',
      participant: '15559999999@s.whatsapp.net',
      fromMe: true,
    },
    messageTimestamp: 1787063003,
    message: { imageMessage: { caption: 'Owner answered with diagram' } },
  },
}), true);
await waitFor(() => spool.queueLength === 0);
const after = readFileSync(spoolPath, 'utf8').trim().split('\n').map(JSON.parse);
assert.equal(after.length, 3);
assert.equal(after[1].kind, 'status');
assert.deepEqual(after[2], {
  schema: 1,
  providerMessageId: 'provider-message-2',
  chatId: '120363001234567890@g.us',
  senderId: 'self',
  senderName: 'self',
  timestamp: 1787063003,
  direction: 'outbound',
  kind: 'group',
  text: 'Owner answered with diagram',
  mediaType: 'image',
  upsertType: 'append',
  groupAddressing: {
    mentionedIds: [],
    quotedParticipant: null,
    quotedMessageId: null,
    ownerAddressed: false,
  },
});

const nativePath = path.join(root, 'native.ndjson');
const nativeSpool = passive.createPassiveIngestSpool({ enabled: true, spoolPath: nativePath });
await waitFor(() => nativeSpool.healthy);
assert.equal(nativeSpool.append({
  type: 'notify',
  msg: {
    key: {
      id: 'provider-location-1',
      remoteJid: 'external@s.whatsapp.net',
      fromMe: false,
    },
    messageTimestamp: 1787063004,
    message: { locationMessage: { degreesLatitude: 40.7, degreesLongitude: -74.0 } },
  },
}), true);
await waitFor(() => nativeSpool.queueLength === 0);
const nativeRow = JSON.parse(readFileSync(nativePath, 'utf8').trim());
assert.equal(nativeRow.text, null);
assert.equal(nativeRow.mediaType, 'location');

console.log('  ✓ every protocol conversation kind persists while group addressing stays explicit');

chmodSync(spoolPath, 0o644);
assert.equal(spool.append({
  type: 'notify',
  msg: {
    key: { id: 'unsafe-1', remoteJid: '15551234567@s.whatsapp.net', fromMe: false },
    messageTimestamp: 1787063004,
    message: { conversation: 'must not append to an unsafe spool' },
  },
}), true);
await waitFor(() => spool.lastError !== null);
assert.match(spool.lastError, /owner-only/);
assert.equal(readFileSync(spoolPath, 'utf8').trim().split('\n').length, 3);
console.log('  ✓ unsafe pre-existing spool permissions fail closed without chmod');

await Promise.all([spool.close(), nativeSpool.close()]);
