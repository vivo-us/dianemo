import crypto from "node:crypto";

const ALGORITHM = "aes-256-gcm";
const KEY_LENGTH = 32;
const IV_LENGTH = 12;
const SALT_LENGTH = 16;
const AUTH_TAG_LENGTH = 16;

/**
 * Derived keys cached by secret and salt, LRU. `scryptSync` at Node's defaults costs
 * ~18 ms and blocks the event loop — stalling the token bucket, the drain and the
 * heartbeats with it — while the hot path decrypts an access token per request. The
 * first derivation still pays full price, which is what the KDF protects.
 */
const MAX_CACHED_KEYS = 1024;
const derivedKeys = new Map<string, Buffer>();

function deriveKey(secret: string, salt: Buffer): Buffer {
  const cacheKey = `${salt.toString("base64")}:${secret}`;
  const cached = derivedKeys.get(cacheKey);
  if (cached) {
    // Re-insert to move it to the end; Map iterates in insertion order, so this
    // is what makes eviction least-recently-used rather than oldest-first.
    derivedKeys.delete(cacheKey);
    derivedKeys.set(cacheKey, cached);
    return cached;
  }

  const key = crypto.scryptSync(secret, salt, KEY_LENGTH);
  if (derivedKeys.size >= MAX_CACHED_KEYS) {
    const oldest = derivedKeys.keys().next();
    if (!oldest.done) derivedKeys.delete(oldest.value);
  }
  derivedKeys.set(cacheKey, key);
  return key;
}

/**
 * One salt per secret, per process, so live derivations track the number of distinct
 * secrets rather than the number of stored credentials — a fresh salt per `encrypt`
 * is a guaranteed cache miss.
 *
 * Still random per process and still travels with the ciphertext, so this costs
 * nothing against precomputation: scrypt's work factor is unchanged.
 */
const encryptionSalts = new Map<string, Buffer>();

function saltForEncrypt(secret: string): Buffer {
  let salt = encryptionSalts.get(secret);
  if (!salt) {
    salt = crypto.randomBytes(SALT_LENGTH);
    if (encryptionSalts.size >= MAX_CACHED_KEYS) encryptionSalts.clear();
    encryptionSalts.set(secret, salt);
  }
  return salt;
}

/**
 * AES-256-GCM. Returns base64 of `salt + iv + authTag + ciphertext`, so the output
 * carries everything `decrypt` needs except the secret.
 */
export function encrypt(plaintext: string, secret: string): string {
  const salt = saltForEncrypt(secret);
  const key = deriveKey(secret, salt);
  const iv = crypto.randomBytes(IV_LENGTH);

  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const ciphertext = Buffer.concat([
    cipher.update(plaintext, "utf8"),
    cipher.final(),
  ]);
  const authTag = cipher.getAuthTag();

  return Buffer.concat([salt, iv, authTag, ciphertext]).toString("base64");
}

/** Inverse of {@link encrypt}. Throws when the secret is wrong or the input is torn. */
export function decrypt(encrypted: string, secret: string): string {
  const data = Buffer.from(encrypted, "base64");

  const salt = data.subarray(0, SALT_LENGTH);
  const iv = data.subarray(SALT_LENGTH, SALT_LENGTH + IV_LENGTH);
  const authTag = data.subarray(
    SALT_LENGTH + IV_LENGTH,
    SALT_LENGTH + IV_LENGTH + AUTH_TAG_LENGTH
  );
  const ciphertext = data.subarray(SALT_LENGTH + IV_LENGTH + AUTH_TAG_LENGTH);

  const key = deriveKey(secret, salt);
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  return Buffer.concat([
    decipher.update(ciphertext),
    decipher.final(),
  ]).toString("utf8");
}
