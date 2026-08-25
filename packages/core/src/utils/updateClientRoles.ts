import type { RequestHandlerMetadata } from "../types.js";
import type { BatchOp } from "../backend/types.js";
import { isSharedLimitOnly } from "./rateLimit.js";
import type RequestHandler from "../index.js";

/**
 * How long an instance registration survives without a heartbeat.
 *
 * Refreshed every second, so this is how many consecutive beats may be missed
 * before peers treat the replica as dead and reclaim its queued work. Three
 * seconds was one ordinary GC pause away from destroying a live replica's
 * requests; ten keeps failover responsive while tolerating a normal stall.
 */
const INSTANCE_TTL_SECONDS = 10;

/**
 * Elects this instance's role for each client it holds.
 *
 * A client already claimed by an older instance makes this one a "worker";
 * anything else makes it the "controller" that drains that client's queue.
 * Ordering instances consistently is what keeps exactly one controller per
 * client without a negotiation round.
 */
async function updateClientRoles(this: RequestHandler, isStartup = false) {
  const clientsBefore = await getClientsBefore.bind(this)();
  let hasChanges = false;
  for (const [name, client] of this.clients) {
    const rateLimit = client.getRateLimit();
    // A `sharedLimit` client has no queue of its own — it was constructed with the
    // owner's name, so the owner's controller drains it. It is the only limit such
    // a client may declare, which is what keeps one controller per budget.
    const borrowsWholeQueue = isSharedLimitOnly(rateLimit);
    const worker = clientsBefore.has(name) || borrowsWholeQueue;
    const newRole = worker ? "worker" : "controller";
    if (client.getRole() === newRole) continue;
    await client.updateRole(newRole);
    hasChanges = true;
  }
  if (!hasChanges && !isStartup) return;
  await updateInstanceRegistration.bind(this)(hasChanges, isStartup);
}

/**
 * Clients already claimed by an instance that outranks this one, so every
 * instance reaches the same answer from the same ordering — higher priority
 * first, then by id as a stable tiebreak.
 */
async function getClientsBefore(this: RequestHandler) {
  const instances = await getInstances.bind(this)();
  const sorted = instances.sort((a, b) => {
    if (a.priority > b.priority) return -1;
    else if (a.priority < b.priority) return 1;
    else return b.id.localeCompare(a.id);
  });
  const clientsBefore: Set<string> = new Set();
  for (const instance of sorted) {
    if (instance.id === this.id) break;
    instance.registeredClients.forEach((c) => clientsBefore.add(c));
  }
  return clientsBefore;
}

/**
 * Every live instance, this one included. The backend drops registrations whose
 * data has expired, so a crashed instance stops counting toward elections.
 */
async function getInstances(this: RequestHandler) {
  const otherInstances = await this.backend.getInstances(
    `${this.namespace}:instances`,
    `${this.namespace}:instance:`,
    this.id
  );

  const instances: RequestHandlerMetadata[] = [this.getMetadata()];
  for (const instance of otherInstances) {
    instances.push(JSON.parse(instance.data));
  }
  return instances;
}

/** Publishes this instance's client list so peers can elect against it. */
async function updateInstanceRegistration(
  this: RequestHandler,
  hasChanges: boolean,
  isStartup: boolean
) {
  const key = `${this.namespace}:instance:${this.id}`;
  const ops: BatchOp[] = [
    {
      op: "set",
      key,
      value: JSON.stringify(this.getMetadata()),
      ttlSeconds: INSTANCE_TTL_SECONDS,
    },
  ];
  if (isStartup) {
    ops.push({
      op: "sadd",
      key: `${this.namespace}:instances`,
      member: this.id,
    });
    ops.push({
      op: "publish",
      channel: `${this.namespace}:instanceStarted`,
      message: this.id,
    });
  }
  if (hasChanges) {
    ops.push({
      op: "publish",
      channel: `${this.namespace}:instanceUpdated`,
      message: this.id,
    });
  }
  await this.backend.batch(ops);
  // After the await, not on entry: a peer's departure elects here, and an
  // election that began before shutdown resumes at this line once doStop has
  // cleared the heartbeat. Nothing clears an interval armed past that point.
  if (this.status !== "started" && this.status !== "starting") return;
  if (this.heartbeatInterval) return;
  this.heartbeatInterval = setInterval(() => {
    // setInterval discards the promise, so an unguarded rejection here — a
    // heartbeat landing mid-shutdown, say — would be unhandled and take the
    // host process down. A missed heartbeat only costs a re-election.
    void this.backend
      .batch([
        // `set`, not `expire`: EXPIRE on an already-expired key is a no-op, so one
        // stall longer than the TTL would drop this replica out of the alive set
        // permanently and let the controller delete its queued work as orphaned.
        {
          op: "set",
          key,
          value: JSON.stringify(this.getMetadata()),
          ttlSeconds: INSTANCE_TTL_SECONDS,
        },
        // Re-asserted every beat, not only at startup: the `set` above restores the
        // registration key after a stall, but a peer's election SREMs the id from
        // this set when it finds that key missing, and nothing else puts it back.
        // Idempotent, and it rides a batch already being sent.
        {
          op: "sadd",
          key: `${this.namespace}:instances`,
          member: this.id,
        },
        {
          op: "publish",
          channel: `${this.namespace}:instanceHeartbeat`,
          message: this.id,
        },
      ])
      .catch((error) => {
        this.logger.error({ error }, "Heartbeat publish failed");
      });
  }, 1000);
}

export default updateClientRoles;
