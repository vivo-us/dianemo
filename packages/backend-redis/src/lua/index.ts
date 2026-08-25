import { getInstances } from "./instances.js";
import {
  acquireConcurrency,
  acquireQueuedConcurrency,
  getConcurrencyState,
  releaseConcurrency,
  tryAdmitConcurrency,
} from "./concurrency.js";
import {
  refundTokens,
  tokenBucket,
  tryAdmitImmediately,
} from "./tokenBucket.js";
import {
  acquireMultiLimit,
  releaseMultiLimit,
  tryAdmitMultiLimit,
} from "./multiLimit.js";
import {
  cleanupStaleFrozenGrants,
  hasThawRequestInProgress,
  readFreezeState,
  tryAdmitNoLimit,
  tryStartThawRequest,
  updateThawProgress,
  writeFreezeState,
} from "./freeze.js";
import {
  addRequest,
  cleanupOrphanedRequests,
  getAllRequests,
  getNextRequest,
  getQueueStats,
  removeRequest,
  updateRequest,
} from "./queue.js";

/**
 * Every Lua script, with the key count it must be registered under.
 *
 * `keys` sits beside the script it belongs to so the two can be checked against
 * each other: it has to equal the highest `KEYS[n]` the body reads. ioredis
 * splits a call's arguments into keys and ARGV on this number alone, so a wrong
 * count silently shifts every ARGV slot along — see `ttlArgvWarning` in
 * `fragments.ts` for how invisible that class of mistake is.
 *
 * `"variadic"` is for the multi-limit scripts alone, whose key count comes from
 * a client's configuration. ioredis then expects the count as the call's first
 * argument instead, so those callers pass it and everything after it shifts by
 * one — `ctx.runVariadic` is the only way to invoke them.
 */
export const SCRIPTS = {
  dianemoTokenBucket: { keys: 1, lua: tokenBucket },
  dianemoAcquireMultiLimit: { keys: "variadic", lua: acquireMultiLimit },
  dianemoReleaseMultiLimit: { keys: "variadic", lua: releaseMultiLimit },
  dianemoTryAdmitMultiLimit: { keys: "variadic", lua: tryAdmitMultiLimit },
  dianemoRefundTokens: { keys: 2, lua: refundTokens },
  dianemoAcquireConcurrency: { keys: 1, lua: acquireConcurrency },
  dianemoAcquireQueuedConcurrency: { keys: 2, lua: acquireQueuedConcurrency },
  dianemoReleaseConcurrency: { keys: 1, lua: releaseConcurrency },
  dianemoGetConcurrencyState: { keys: 1, lua: getConcurrencyState },
  dianemoGetNextRequest: { keys: 2, lua: getNextRequest },
  dianemoUpdateRequest: { keys: 2, lua: updateRequest },
  dianemoUpdateThawProgress: { keys: 2, lua: updateThawProgress },
  dianemoWriteFreezeState: { keys: 1, lua: writeFreezeState },
  dianemoReadFreezeState: { keys: 1, lua: readFreezeState },
  dianemoTryAdmitImmediately: { keys: 3, lua: tryAdmitImmediately },
  dianemoTryAdmitNoLimit: { keys: 2, lua: tryAdmitNoLimit },
  dianemoTryAdmitConcurrency: { keys: 3, lua: tryAdmitConcurrency },
  dianemoAddRequest: { keys: 3, lua: addRequest },
  dianemoRemoveRequest: { keys: 3, lua: removeRequest },
  dianemoGetQueueStats: { keys: 2, lua: getQueueStats },
  dianemoGetAllRequests: { keys: 2, lua: getAllRequests },
  dianemoCleanupOrphanedRequests: { keys: 2, lua: cleanupOrphanedRequests },
  dianemoHasThawRequestInProgress: { keys: 2, lua: hasThawRequestInProgress },
  dianemoTryStartThawRequest: { keys: 4, lua: tryStartThawRequest },
  dianemoGetInstances: { keys: 1, lua: getInstances },
  dianemoCleanupStaleFrozenGrants: { keys: 3, lua: cleanupStaleFrozenGrants },
} as const;

/** The registered command names, so a typo is a compile error rather than a throw. */
export type ScriptName = keyof typeof SCRIPTS;
