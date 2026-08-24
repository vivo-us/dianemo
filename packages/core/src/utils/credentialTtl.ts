// The TTL is garbage collection, not expiry enforcement — `expiresAt` decides
// whether a token may be used — so a generous value costs only memory. Too short is
// the dangerous direction: both tokens share one hash and the TTL covers the whole
// key, so sizing from the access token discards the refresh token with it.

/**
 * Assumed lifetime for a refresh token whose expiry the provider did not state.
 *
 * Most providers issue refresh tokens that are long-lived or do not expire at
 * all, and none of them expect the client to discard one after an hour. Ninety
 * days is well past any access-token lifetime while still letting genuinely
 * abandoned credentials fall out of the store on their own.
 */
export const DEFAULT_REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 90;

/** Keeps a key alive at least a minute, so a short-lived token is still usable. */
const MINIMUM_TTL_SECONDS = 60;

/** Grace period so a key never lapses at the same instant its token does. */
const TTL_BUFFER_SECONDS = 60;

/**
 * Ten years, well past any credential lifetime and far below the ~9.2e15 seconds
 * Redis accepts for `EXPIRE`.
 *
 * `refresh_token_expires_in` is untrusted provider input checked only for being
 * finite and positive, and a "never expires" sentinel such as `9999999999999999`
 * exceeds that ceiling. Redis then rejects the `EXPIRE` after the credential write
 * has already applied, so the grant persists with no expiry at all.
 */
const MAXIMUM_TTL_SECONDS = 60 * 60 * 24 * 365 * 10;

/**
 * @param accessTokenTtlSeconds Seconds until the access token expires.
 * @param hasRefreshToken Whether a refresh token is stored in the same hash.
 * @param refreshTokenTtlSeconds Seconds until the refresh token expires, when known.
 */
export function credentialTtlSeconds(
  accessTokenTtlSeconds: number,
  hasRefreshToken: boolean,
  refreshTokenTtlSeconds?: number
): number {
  const accessTtl = Number.isFinite(accessTokenTtlSeconds)
    ? accessTokenTtlSeconds
    : 0;

  let ttl = accessTtl;
  if (hasRefreshToken) {
    const refreshTtl =
      refreshTokenTtlSeconds !== undefined &&
      Number.isFinite(refreshTokenTtlSeconds)
        ? refreshTokenTtlSeconds
        : DEFAULT_REFRESH_TOKEN_TTL_SECONDS;
    ttl = Math.max(ttl, refreshTtl);
  }

  return Math.min(
    Math.max(Math.ceil(ttl) + TTL_BUFFER_SECONDS, MINIMUM_TTL_SECONDS),
    MAXIMUM_TTL_SECONDS
  );
}
