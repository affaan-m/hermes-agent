import { strict as assert } from 'node:assert';
import {
  chmodSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

import { createPassiveIngestSpool } from './passive_ingest.js';

const root = mkdtempSync(path.join(tmpdir(), 'hermes-wa-worker-'));
const message = (id) => ({
  type: 'notify',
  msg: {
    key: { id, remoteJid: '15551234567@s.whatsapp.net', fromMe: false },
    messageTimestamp: 1787063000,
    message: { conversation: id },
  },
});

async function waitFor(predicate, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('timed out waiting for spool state');
    await delay(10);
  }
}

const preflightSettled = (candidate) => candidate.healthy
  || (candidate.lastError !== null && !candidate.lastError.includes('preflight pending'));

const spoolPath = path.join(root, 'async.ndjson');
const spool = createPassiveIngestSpool({ enabled: true, spoolPath, maxQueue: 8 });
assert.equal(spool.healthy, false, 'enabled health must start false before preflight');
await waitFor(() => spool.healthy);
assert.equal(spool.append(message('one')), true);
await waitFor(() => readFileSync(spoolPath, 'utf8').includes('"one"'));
await spool.close();

const fifoPath = path.join(root, 'blocked-fifo');
assert.equal(spawnSync('mkfifo', [fifoPath]).status, 0);
const fifo = createPassiveIngestSpool({ enabled: true, spoolPath: fifoPath });
let timerFired = false;
setTimeout(() => { timerFired = true; }, 0);
await waitFor(() => preflightSettled(fifo));
await delay(0);
assert.equal(timerFired, true, 'FIFO preflight must not block the Node event loop');
assert.match(fifo.lastError, /regular|non-blocking|spool|ENXIO/i);
await fifo.close();

const directoryPath = path.join(root, 'directory');
mkdirSync(directoryPath);
const directory = createPassiveIngestSpool({ enabled: true, spoolPath: directoryPath });
await waitFor(() => preflightSettled(directory));
assert.equal(directory.healthy, false);
await directory.close();

const unsafePath = path.join(root, 'unsafe.ndjson');
const unsafe = createPassiveIngestSpool({ enabled: true, spoolPath: unsafePath });
await waitFor(() => unsafe.healthy);
await unsafe.close();
chmodSync(unsafePath, 0o644);
const reopenedUnsafe = createPassiveIngestSpool({ enabled: true, spoolPath: unsafePath });
await waitFor(() => preflightSettled(reopenedUnsafe));
assert.match(reopenedUnsafe.lastError, /owner-only/);
await reopenedUnsafe.close();

const boundedPath = path.join(root, 'bounded.ndjson');
const bounded = createPassiveIngestSpool({ enabled: true, spoolPath: boundedPath, maxQueue: 1 });
assert.equal(bounded.append(message('queued-before-ready')), true);
assert.equal(bounded.append(message('queue-overflow')), false);
assert.match(bounded.lastError, /queue capacity/);
await waitFor(() => bounded.queueLength === 0);
assert.match(bounded.lastError, /queue capacity/, 'older success must not erase a later drop');
assert.equal(bounded.append(message('recovery-after-observed-failure')), true);
await waitFor(() => bounded.queueLength === 0);
assert.equal(bounded.healthy, true);
await bounded.close();

console.log('  ✓ bounded worker preflights securely and never blocks bridge routing');
