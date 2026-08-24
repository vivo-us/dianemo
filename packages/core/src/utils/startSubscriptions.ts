import type { RequestDoneData, RequestMetadata } from "../request/types.js";
import type { RateLimitUpdatedData } from "../client/types.js";
import { destroyTemplateClients } from "./templateClients.js";
import type RequestHandler from "../index.js";

async function startSubscriptions(this: RequestHandler) {
  // Idempotent, because `handleRequest` retries `start()` on every request while
  // the status is not "started": without the guard each attempt adds another handler
  // to the same pub/sub connection, unbounded in request volume.
  if (this.subscriptionsStarted) return;
  this.subscriptionsStarted = true;

  const onMessage = handleMessage.bind(this);
  await this.backend.subscribe(
    [
      `${this.namespace}:instanceStarted`,
      `${this.namespace}:instanceUpdated`,
      `${this.namespace}:instanceHeartbeat`,
      `${this.namespace}:instanceStopped`,
      `${this.namespace}:templateClientAdded`,
      `${this.namespace}:templateClientRemoved`,
      `${this.namespace}:requestAdded`,
      `${this.namespace}:requestReady:${this.id}`,
      `${this.namespace}:requestDone`,
      `${this.namespace}:rateLimitUpdated`,
    ],
    (channel: string, message: string) => {
      // The subscriber discards the returned promise, so a rejection here would
      // be unhandled and terminate the host process. A pub/sub message is never
      // worth taking someone else's server down for.
      void onMessage(channel, message).catch((error) => {
        this.logger.error(
          { error, channel },
          "Failed to handle a dianemo pub/sub message"
        );
      });
    }
  );
}

/**
 * Truncates a pub/sub payload for a log line without assuming it is a string.
 *
 * These previews live inside the catch blocks that report a malformed payload, so
 * `message.substring` threw on exactly the input the handler existed to describe —
 * turning a logged parse failure into an unhandled error, and masking whatever
 * sent the bad payload in the first place.
 */
function preview(message: unknown): string {
  return typeof message === "string"
    ? message.slice(0, 200)
    : `[${typeof message}] ${String(message).slice(0, 200)}`;
}

async function handleMessage(
  this: RequestHandler,
  channel: string,
  message: string
) {
  switch (channel) {
    case `${this.namespace}:instanceStarted`:
      await handleInstanceStarted.bind(this)(message);
      break;
    case `${this.namespace}:instanceUpdated`:
      await handleInstanceUpdated.bind(this)(message);
      break;
    case `${this.namespace}:instanceHeartbeat`:
      await handleInstanceHeartbeat.bind(this)(message);
      break;
    case `${this.namespace}:instanceStopped`:
      await handleInstanceStopped.bind(this)(message);
      break;
    case `${this.namespace}:templateClientAdded`:
      await handleTemplateClientAdded.bind(this)(message);
      break;
    case `${this.namespace}:templateClientRemoved`:
      await handleTemplateClientRemoved.bind(this)(message);
      break;
    // Awaited, not left to settle on their own: these three reach the backend, the
    // wrapper below only observes `handleMessage`, and a message landing while the
    // connection closes — ordinary at shutdown — would reject unhandled.
    case `${this.namespace}:requestAdded`:
      await handleRequestAdded.bind(this)(message);
      break;
    case `${this.namespace}:requestReady:${this.id}`:
      handleRequestReady.bind(this)(message);
      break;
    case `${this.namespace}:requestDone`:
      await handleRequestDone.bind(this)(message);
      break;
    case `${this.namespace}:rateLimitUpdated`:
      handleRateLimitUpdated.bind(this)(message);
      break;
    default:
      return;
  }
}

/**
 * How long a peer may go silent before it is presumed dead.
 *
 * Must stay longer than the registration TTL in `updateClientRoles`. Equal
 * values expire in a dead heat off the same heartbeat, so an election could
 * still see the dead registration and conclude nothing had changed.
 */
const PEER_TIMEOUT_MS = 12000;

async function handleInstanceStarted(this: RequestHandler, message: string) {
  if (this.id === message) return;
  this.heartbeatTimeouts.set(
    message,
    setTimeout(() => {
      // A new async context, outside the `.catch` guarding pub/sub dispatch —
      // and it fires exactly when the backend is most likely to be failing.
      void (async () => {
        this.logger.warn(
          `Instance ${message} has not sent a heartbeat in ${PEER_TIMEOUT_MS}ms`
        );
        await handleInstanceStopped.bind(this)(message);
      })().catch((error) => {
        this.logger.error(
          { error },
          `Failed to handle presumed loss of instance ${message}`
        );
      });
    }, PEER_TIMEOUT_MS)
  );
  await this.scheduleClientRoles();
}

async function handleInstanceUpdated(this: RequestHandler, message: string) {
  if (this.id === message) return;
  await this.scheduleClientRoles();
}

async function handleInstanceHeartbeat(this: RequestHandler, message: string) {
  if (this.id === message) return;
  const timeout = this.heartbeatTimeouts.get(message);
  if (timeout) timeout.refresh();
  else await handleInstanceStarted.bind(this)(message);
}

async function handleInstanceStopped(this: RequestHandler, message: string) {
  if (this.id === message) return;
  const timeout = this.heartbeatTimeouts.get(message);
  if (timeout) clearTimeout(timeout);
  this.heartbeatTimeouts.delete(message);

  // Election first: it is what prunes the departed id from the alive set, and
  // the sweep reads that set to decide what is orphaned. Sweeping first meant
  // the dead instance was still a member, so the first sweep after a departure
  // reclaimed nothing and recovery waited for the next health tick.
  await this.scheduleClientRoles();

  for (const client of this.clients.values()) {
    await client.cleanupOrphanedRequests();
  }
}

async function handleTemplateClientAdded(
  this: RequestHandler,
  message: string
) {
  let data: { templateName: string; instanceId: string };
  try {
    data = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleTemplateClientAdded"
    );
    return;
  }
  await this.rebuildTemplateClient(data.templateName, data.instanceId);
  await this.scheduleClientRoles();
}

async function handleTemplateClientRemoved(
  this: RequestHandler,
  message: string
) {
  let data: { templateName: string; instanceId: string };
  try {
    data = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleTemplateClientRemoved"
    );
    return;
  }
  await destroyTemplateClients.bind(this)(data.templateName, data.instanceId);
}

async function handleRequestAdded(this: RequestHandler, message: string) {
  let data: { clientName: string };
  try {
    data = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleRequestAdded"
    );
    return;
  }
  const client = this.clients.get(data.clientName);
  if (client) await client.processRequests();
}

function handleRequestReady(this: RequestHandler, message: string) {
  let readyData: RequestMetadata;
  try {
    readyData = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleRequestReady"
    );
    return;
  }
  const readyClient = this.clients.get(readyData.clientName);
  if (readyClient) readyClient.handleRequestReady(readyData);
}

async function handleRequestDone(this: RequestHandler, message: string) {
  let doneData: RequestDoneData;
  try {
    doneData = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleRequestDone"
    );
    return;
  }
  // Every replica records the notification. Workers retain it across an
  // election gap; the active controller performs single-flight cleanup.
  const doneClient = this.clients.get(doneData.clientName);
  if (doneClient) await doneClient.finalizeOwnedRequest(doneData);
}

async function handleRateLimitUpdated(this: RequestHandler, message: string) {
  let updatedData: RateLimitUpdatedData;
  try {
    updatedData = JSON.parse(message);
  } catch (error) {
    this.logger.error(
      {
        error,
        message: preview(message),
      },
      "Failed to parse JSON in handleRateLimitUpdated"
    );
    return;
  }
  const client = this.clients.get(updatedData.clientName);
  if (client) client.handleRateLimitUpdated(updatedData);
}

export default startSubscriptions;
