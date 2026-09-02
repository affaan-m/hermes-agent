export class SendTimeoutError extends Error {}

export function createReconnectCoordinator({
  setTimer = setTimeout,
  clearTimer = clearTimeout,
} = {}) {
  const timers = new WeakMap();
  const starts = new WeakMap();

  function cancel(owner) {
    const timer = timers.get(owner);
    if (timer !== undefined) clearTimer(timer);
    timers.delete(owner);
    return starts.get(owner);
  }

  function schedule(owner, delay, start) {
    const existing = starts.get(owner);
    if (existing) return existing;
    cancel(owner);
    const timer = setTimer(() => {
      timers.delete(owner);
      const inFlight = Promise.resolve().then(() => start(owner));
      starts.set(owner, inFlight);
      const clear = () => {
        if (starts.get(owner) === inFlight) starts.delete(owner);
      };
      inFlight.then(clear, clear);
    }, delay);
    timers.set(owner, timer);
  }

  return Object.freeze({ cancel, schedule });
}

/**
 * Cancel any reconnect owned by the timed-out generation, then wait until the
 * socket that is globally current is the same socket whose readiness resolved.
 * A reconnect that won the race before the timeout is reused, but never before
 * its own open promise resolves.
 */
export async function awaitReadySocketGeneration(timedOutSocket, {
  cancelPendingReconnect,
  getCurrentSocket,
  getReadiness,
  isRetired,
  startReplacement,
}) {
  cancelPendingReconnect(timedOutSocket);
  for (;;) {
    const candidate = getCurrentSocket();
    let readySocket;
    if (!candidate || candidate === timedOutSocket || isRetired(candidate)) {
      readySocket = await startReplacement();
    } else {
      const readiness = getReadiness(candidate);
      if (!readiness) throw new Error('current socket has no readiness generation');
      readySocket = await readiness;
    }
    const current = getCurrentSocket();
    if (current === readySocket && current !== timedOutSocket && !isRetired(current)) {
      return current;
    }
  }
}

function timeoutAfter(ms) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new SendTimeoutError(`send timed out after ${ms}ms`)), ms);
  });
}

/**
 * Serialize transport sends independently from caller-visible timeouts.
 *
 * Without an onTimeout transport-retirement callback, the queue remains owned
 * until the underlying send settles, preventing overlap. With onTimeout, that
 * callback must retire the old transport and establish a replacement before it
 * resolves; only then may a new queue generation proceed while the abandoned
 * promise is observed in the background.
 */
export function createSerializedSender(send, { onTimeout = null } = {}) {
  if (typeof send !== 'function') throw new TypeError('send must be a function');
  if (onTimeout !== null && typeof onTimeout !== 'function') {
    throw new TypeError('onTimeout must be a function');
  }
  let queue = Promise.resolve();

  return function sendSerialized(chatId, payload, timeoutMs) {
    const scheduled = queue.then(async () => {
      let operation;
      try {
        operation = send(chatId, payload);
      } catch (error) {
        operation = Promise.reject(error);
      }
      const structured = operation && typeof operation === 'object' && 'promise' in operation;
      const context = structured ? operation.context : undefined;
      const underlying = Promise.resolve(structured ? operation.promise : operation);
      try {
        return await Promise.race([underlying, timeoutAfter(timeoutMs)]);
      } catch (error) {
        if (!(error instanceof SendTimeoutError)) throw error;
        if (onTimeout === null) {
          try {
            await underlying;
          } catch {
            // The caller still receives the timeout that happened first.
          }
          throw error;
        }
        await onTimeout(error, context);
        // The retired transport may never settle. Observe any eventual
        // rejection without retaining queue ownership across generations.
        underlying.catch(() => {});
        throw error;
      }
    });
    queue = scheduled.catch(() => {});
    return scheduled;
  };
}
