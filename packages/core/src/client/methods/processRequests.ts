import type { RequestMetadata } from "../../request/types.js";
import type BaseClient from "../index.js";
import {
  ClientFrozenError,
  RequestCostExceedsBudgetError,
} from "../../errors.js";

/** A cost above the ceiling is not waiting for capacity — nothing will fit it. */
function isPermanentlyUnsatisfiable(
  client: BaseClient,
  request: RequestMetadata
): boolean {
  const ceiling = client.getCostCeilingForQueue();
  return ceiling !== undefined && (request.cost ?? 1) > ceiling;
}

/**
 * Fails a queued request whose cost the current budget can never satisfy.
 *
 * Only the owning replica holds the waiter, so elsewhere the admission timeout
 * remains the backstop and the entry is left for it to clean up.
 */
async function failUnsatisfiable(
  this: BaseClient,
  request: RequestMetadata,
  known?: RequestCostExceedsBudgetError
): Promise<void> {
  if (request.ownerId !== this.instanceId) return;

  // Re-checked against the ceiling as it stands now: reaching here costs two
  // awaited backend calls, an ordinary window for a `rateLimitChange` to arrive,
  // and a ceiling that dips and recovers inside it must not turn a transient dip
  // into a permanent rejection.
  if (!isPermanentlyUnsatisfiable(this, request)) return;

  const ceiling = this.getCostCeilingForQueue();
  const error =
    known ??
    new RequestCostExceedsBudgetError(
      this.name,
      request.cost ?? 1,
      ceiling ?? 0
    );
  if (this.failWaitingRequest(request.requestId, error)) {
    await this.removeRequestFromQueue(request.requestId).catch(() => {});
  }
}

async function processRequests(this: BaseClient) {
  if (this.role === "worker") return;
  // Shutdown has stopped admitting. Checked here rather than only by not poking the
  // loop, because a completing request's `requestDone` and any booked drain timer
  // both still arrive while the handler is tearing down, and either would dispatch
  // fresh work upstream after the point of no return.
  if (this.admissionHalted) return;

  if (this.processingLock) {
    this.processingPending = true;
    return;
  }

  this.processingLock = true;
  this.processingPending = false;

  try {
    // Grants that cannot proceed right now. `getNextRequest` skips them, so a
    // blocked grant neither stalls the ones behind it nor is handed back on the
    // next iteration — without which the loop spins for the whole freeze window.
    const blockedGrantIds = new Set<string>();
    // Without this the loop would re-select the same skipped head and spin.
    const skippedRequestIds = new Set<string>();

    while (true) {
      const frozenGrantIds = await this.getFrozenGrantIds();
      const request = await this.getNextRequest(
        [...new Set([...frozenGrantIds, ...blockedGrantIds])],
        skippedRequestIds.size ? [...skippedRequestIds] : undefined
      );
      if (!request) break;
      const { grantId, requestId } = request;

      const requestMetadata: RequestMetadata = {
        requestId: request.requestId,
        clientName: request.clientName,
        requestName: request.requestName,
        status: "inProgress",
        priority: request.priority,
        cost: request.cost,
        retries: request.retries,
        timestamp: request.timestamp,
        grantId: grantId,
        isThawRequest: request.isThawRequest,
        ownerId: request.ownerId,
      };

      const res = await this.canProcessNextRequest(requestMetadata);
      if (!res.canProcess) {
        await this.updateRequestInQueue(requestId, { status: "pending" });
        // Checked before the grant short-circuit below, because an impossible cost
        // is a property of the request rather than of its grant's momentary
        // capacity — otherwise a grant-scoped request goes to `blockedGrantIds` and
        // waits out the admission timeout for an answer known now.
        if (isPermanentlyUnsatisfiable(this, requestMetadata)) {
          this.logger.warn(
            `Client ${this.name} | request ${requestId} has a cost the client can never satisfy; failing it and moving on`
          );
          skippedRequestIds.add(requestId);
          await failUnsatisfiable.bind(this)(requestMetadata);
          continue;
        }

        // Booked above all three exits below, not beside the `break`: two are
        // `continue`s taken by grant-scoped and grant-isolated requests, so booking
        // only before the `break` leaves exactly the isolated-grant tenants
        // recovering on the health tick. A refusal that is not a freeze carries no
        // deadline and books nothing — a concurrency client's occupied slot is woken
        // by the release instead.
        await this.scheduleDrainForFreeze(res.frozenUntil, grantId);

        if (grantId) {
          blockedGrantIds.add(grantId);
          continue;
        }
        // The client-level budget is exhausted or frozen, so nothing drawing on it
        // can proceed — but a grant-isolated client meters each grant against its
        // own bucket, so breaking here would stall every tenant behind a
        // client-level head. Skip it and keep scanning instead; anything else on the
        // client-level budget declines in turn and is skipped too.
        if (this.usesGrantIsolation()) {
          skippedRequestIds.add(requestId);
          continue;
        }
        break;
      }

      // Applies with or without a grant. Grants are opt-in, so gating this on
      // `grantId` would let the default client release its entire queue the
      // instant a freeze expired, instead of probing with exactly one request.
      if (res.isThawRequest) {
        const thawResult = await this.tryStartThawRequest(grantId, requestId);

        if (thawResult === "exists") {
          await this.updateRequestInQueue(requestId, { status: "pending" });
          // No `scheduleDrainForFreeze` here, unlike the three decline exits above:
          // this request is not waiting for the freeze to lapse but for a probe that
          // is already running, and that probe's `requestDone` re-enters this loop.
          break;
        }

        requestMetadata.isThawRequest = true;
      }

      // Between the claim above and the notification below the request exists
      // only in this loop — `getNextRequest` already marked it `inProgress`, so
      // no other controller reconsiders it and orphan cleanup skips it while its
      // owner lives. An escaping rejection would strand it until it timed out.
      try {
        const turn = await this.tryAcquireTurn(requestMetadata);
        if (!turn.acquired) {
          await this.updateRequestInQueue(requestId, { status: "pending" });
          // Coming back later is the whole point: sleeping here would hold the
          // lock and stall every other grant and every cheaper or
          // higher-priority request behind this one.
          this.scheduleDrain(turn.waitTime ?? 0);
          if (grantId) {
            blockedGrantIds.add(grantId);
            continue;
          }
          // Same reasoning as the `canProcess` branch above, repeated because that
          // branch only fires for a freeze: a token bucket discovers it has no
          // tokens here, one step later.
          if (this.usesGrantIsolation()) {
            skippedRequestIds.add(requestId);
            continue;
          }
          break;
        }

        // The request may have completed between the claim above and here.
        // Admission excludes a request's own id from the occupancy sum so a
        // resubmit does not compete with itself, which means a claim for an id
        // that has just released succeeds and re-creates its entry — and nothing
        // is left to release it, so the capacity sits claimed until its TTL.
        //
        // Narrows the window rather than closing it: a completion landing between
        // this read and the notification below still strands the claim. Closing
        // that would need the claim and the confirmation in one atomic operation,
        // which is impossible while the claim and the queue entry are separate
        // keys written by different callers.
        //
        // A read that failed is not evidence the request is gone, so an
        // unreadable entry is treated as still queued: keeping the claim is the
        // recoverable error.
        if (this.claimsReleasableCapacity()) {
          const stillQueued = await this.getRequestFromQueue(requestId).catch(
            (error: unknown) => {
              this.logger.warn(
                { error },
                `Client ${this.name} | could not confirm request ${requestId} is still queued; keeping its claim and continuing`
              );
              return "unreadable" as const;
            }
          );
          // Only a definite absence hands the claim back.
          if (stillQueued === null) {
            await this.releaseUnusedAdmission(requestMetadata);
            continue;
          }
        }

        // Addressed to the replica waiting on this request rather than broadcast,
        // since only the owner has a listener for it. When that owner is this
        // replica the hand-over stays in process.
        if (requestMetadata.ownerId === this.instanceId) {
          this.handleRequestReady(requestMetadata);
        } else {
          await this.backend.publish(
            `${this.handlerNamespace}:requestReady:${requestMetadata.ownerId}`,
            JSON.stringify(requestMetadata)
          );
        }
      } catch (error) {
        await this.updateRequestInQueue(requestId, {
          status: "pending",
        }).catch(() => {});

        // A ceiling lowered while this request sat in the queue makes its cost
        // impossible. Stepping over it is what keeps everything behind it
        // moving; breaking here would re-select the same head every pass and
        // the client would never admit anything again.
        if (error instanceof RequestCostExceedsBudgetError) {
          this.logger.warn(
            `Client ${this.name} | request ${requestId} costs more than the current budget allows; failing it and moving on`
          );
          skippedRequestIds.add(requestId);
          await failUnsatisfiable.bind(this)(requestMetadata, error);
          continue;
        }

        // A freeze landing mid-wait is ordinary control flow: the request goes
        // back and the thaw path re-admits it. The second freeze exit from this
        // loop, and it books the same wake-up as the first — the throw comes from
        // inside admission, so the deadline is not in hand here and this path
        // alone pays a read for it.
        if (error instanceof ClientFrozenError) {
          await this.scheduleDrainForFreeze(undefined, grantId);
        } else {
          this.logger.error(
            { error },
            `Client ${this.name} | admission failed for request ${requestId}; returned it to the queue`
          );
        }
        break;
      }
    }
  } finally {
    this.processingLock = false;
    if (this.processingPending) {
      this.processingPending = false;
      setImmediate(() =>
        this.processRequests().catch((error) => {
          this.logger.error(
            { error },
            `Client ${this.name} | processRequests failed`
          );
        })
      );
    }
  }
}

export default processRequests;
