import type { AxiosError } from "axios";

/**
 * Credential redaction for anything this library logs or throws.
 *
 * The rule throughout: prefer an allowlist of what may be shown over a denylist of
 * what must be hidden, because a denylist of *names* cannot cover data whose names
 * this library does not choose.
 */

export const REDACTED = "[REDACTED]";

/** Well-known credential headers. A backstop, not the primary defence. */
const SENSITIVE_HEADER_NAMES = new Set([
  "authorization",
  "proxy-authorization",
  "www-authenticate",
  "proxy-authenticate",
  "x-payment-authorization-token",
  "x-api-key",
  "api-key",
  "apikey",
  "x-auth-token",
  "x-access-token",
  "x-session-token",
  "x-amz-security-token",
  "x-amz-signature",
  "x-goog-api-key",
  "x-functions-key",
  "x-csrf-token",
  "x-xsrf-token",
  "dd-api-key",
  "subscription-key",
  "ocp-apim-subscription-key",
  "private-token",
  "token",
  "cookie",
  "set-cookie",
]);

const SENSITIVE_KEY_PATTERN =
  /^(client_?secret|refresh_?token|access_?token|id_?token|password|passwd|api_?key|secret|token|sig|signature|code|code_?verifier|client_?assertion|assertion|private_?key|session|auth|key|credential|credentials|passphrase)$/i;

/** Any prefix on a credential-ish word: `subscription-key`, `app_secret`. */
const SENSITIVE_KEY_SUFFIX =
  /[-_.](key|secret|token|password|passwd|auth|sig|signature|credential|credentials)$/i;

/** Whether a header, query parameter or body field name should be hidden. */
function isSensitiveName(
  name: string,
  extraNames?: ReadonlySet<string>
): boolean {
  const lower = name.toLowerCase();
  return (
    SENSITIVE_HEADER_NAMES.has(lower) ||
    SENSITIVE_KEY_PATTERN.test(lower) ||
    SENSITIVE_KEY_SUFFIX.test(lower) ||
    Boolean(extraNames?.has(lower))
  );
}

/**
 * Values that are recognisably credentials whatever they are called — a vendor
 * echoing back `{"echoed_authorization": "Bearer …"}`, a JWT inside a message.
 */
const SENSITIVE_VALUE_PATTERNS: RegExp[] = [
  /^(bearer|basic|token|negotiate)\s+\S+/i,
  /^eyJ[\w-]+\.[\w-]+\.[\w-]*$/, // JWT
  /^sk-[A-Za-z0-9_-]{16,}$/, // OpenAI-style
  /^ghp_[A-Za-z0-9]{20,}$/, // GitHub PAT
  /^xox[bpasr]-[A-Za-z0-9-]{10,}$/, // Slack
  /^shpat_[A-Za-z0-9]{20,}$/, // Shopify
  /^AKIA[0-9A-Z]{16}$/, // AWS access key id
  /^AIza[0-9A-Za-z_-]{35}$/, // Google API key
];

function looksSensitive(value: string): boolean {
  return SENSITIVE_VALUE_PATTERNS.some((pattern) => pattern.test(value.trim()));
}

/**
 * @param extraNames Additional header names to redact, lowercased — whatever the
 * client configured, since this library is what puts the value on the wire.
 */
export function redactHeaders(
  headers: Record<string, unknown> | undefined,
  extraNames?: ReadonlySet<string>
): Record<string, unknown> | undefined {
  if (!headers) return headers;
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(headers)) {
    const sensitiveValue = typeof value === "string" && looksSensitive(value);
    out[key] =
      isSensitiveName(key, extraNames) || sensitiveValue ? REDACTED : value;
  }
  return out;
}

/**
 * Strips credentials from a URL's query string and userinfo.
 *
 * Redacting `params` is not enough on its own: an OAuth refresh built with
 * `dataLocation: "urlQuery"` puts `client_secret` inline in `url` instead.
 */
export function redactUrl(url: string | undefined): string | undefined {
  if (!url) return url;

  // `https://user:password@host/…`, and the no-colon form too: a PAT or bearer used
  // as the username (`https://ghp_xxx@host/…`) is the whole credential and has no
  // password component to strip, so userinfo without a colon is replaced wholesale.
  // Scheme-relative `//user@host` is covered as well.
  const out = url
    .replace(
      /^([a-z][a-z0-9+.-]*:\/\/|\/\/)([^/@]*:)([^/@]*)@/i,
      (_m, scheme: string, user: string) => `${scheme}${user}${REDACTED}@`
    )
    // No colon means the userinfo IS the credential — a PAT or bearer used as
    // the username — so there is no password half to strip and the whole
    // component goes.
    .replace(
      /^([a-z][a-z0-9+.-]*:\/\/|\/\/)([^/@:]+)@/i,
      (_m, scheme: string) => `${scheme}${REDACTED}@`
    );

  const queryStart = out.indexOf("?");
  if (queryStart === -1) return out;
  const base = out.slice(0, queryStart);

  // `append`, not `set`, so repeated parameters survive. Never re-encoded
  // either: decoding turns an escaped "&client_secret=" inside a value into what
  // reads as a real parameter.
  const source = new URLSearchParams(out.slice(queryStart + 1));
  const rebuilt = new URLSearchParams();
  let touched = false;
  for (const [key, value] of source.entries()) {
    if (isSensitiveName(key) || looksSensitive(value)) {
      rebuilt.append(key, REDACTED);
      touched = true;
    } else {
      rebuilt.append(key, value);
    }
  }
  if (!touched && out === url) return url;
  return `${base}?${rebuilt.toString()}`;
}

/**
 * Redacts a form-encoded body per parameter, using the same name test as the object
 * branch — this is the shape an OAuth token request uses, so it sees the most
 * credentials, and any weaker test here prints what the object branch would hide.
 * Only the offending value is replaced.
 */
function redactFormEncoded(value: string): string {
  if (!value.includes("=")) return value;
  let touched = false;
  const parts = value.split("&").map((pair) => {
    const eq = pair.indexOf("=");
    if (eq === -1) return pair;
    const name = pair.slice(0, eq);
    const raw = pair.slice(eq + 1);
    let decoded: string;
    try {
      decoded = decodeURIComponent(name.replace(/\+/g, " "));
    } catch {
      decoded = name;
    }
    if (!isSensitiveName(decoded) && !looksSensitive(safeDecode(raw))) {
      return pair;
    }
    touched = true;
    return `${name}=${REDACTED}`;
  });
  return touched ? parts.join("&") : value;
}

/**
 * Redacts a JSON body already serialized to a string. Axios replaces an object
 * `config.data` with its serialized form before dispatch and the error carries that
 * config, so by the time redaction runs a JSON body is a string with no `=` in it —
 * which neither the value-shape test nor the form parser can handle.
 *
 * Undefined when the string is not JSON, so the caller falls through to that parser.
 */
function redactJsonString(
  value: string,
  seen: WeakSet<object>
): string | undefined {
  const trimmed = value.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== "object") return undefined;
  try {
    return JSON.stringify(redactBody(parsed, seen));
  } catch {
    // A body that cannot be re-serialized is not worth risking inside a catch.
    return REDACTED;
  }
}

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value.replace(/\+/g, " "));
  } catch {
    return value;
  }
}

/**
 * Redacts a body by key name and by value shape. Tracks visited objects, since
 * a cyclic `config.data` would otherwise throw from inside a catch block.
 */
export function redactBody(
  value: unknown,
  seen = new WeakSet<object>()
): unknown {
  if (value == null) return value;

  if (typeof value === "string") {
    if (looksSensitive(value)) return REDACTED;
    const asJson = redactJsonString(value, seen);
    if (asJson !== undefined) return asJson;
    return redactFormEncoded(value);
  }

  if (typeof value !== "object") return value;
  if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
  if (seen.has(value)) return "[Circular]";
  seen.add(value);

  if (Array.isArray(value)) return value.map((v) => redactBody(v, seen));

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    out[k] = isSensitiveName(k) ? REDACTED : redactBody(v, seen);
  }
  return out;
}

/**
 * Reduces a request config to the few fields worth logging, each redacted. A
 * whitelist, so `auth`, the agents and the transforms cannot leak by omission.
 */
export function redactConfig(
  config: AxiosError["config"],
  extraHeaderNames?: ReadonlySet<string>
): unknown {
  if (!config) return config;
  return {
    method: config.method,
    baseURL: redactUrl(config.baseURL),
    url: redactUrl(config.url),
    params: redactBody(config.params),
    headers: redactHeaders(
      config.headers as Record<string, unknown> | undefined,
      extraHeaderNames
    ),
    data: redactBody(config.data),
  };
}

/**
 * Strips credentials from an error before it is thrown to the caller.
 *
 * `JSON.stringify(err)` reaches `AxiosError.prototype.toJSON`, which returns
 * `config` verbatim, and `util.inspect(err, { depth: null })` walks `request`,
 * whose `_header` holds the bytes that went over the wire.
 *
 * Mutates in place and returns the same error, so `instanceof` at the catch site
 * still works.
 */
export function sanitizeError(
  error: unknown,
  extraHeaderNames?: ReadonlySet<string>
): unknown {
  if (!error || typeof error !== "object") return error;
  const err = error as {
    config?: AxiosError["config"];
    request?: unknown;
    response?: {
      headers?: unknown;
      data?: unknown;
      request?: unknown;
      statusText?: unknown;
    };
  };

  const safeConfig = err.config
    ? (redactConfig(err.config, extraHeaderNames) as AxiosError["config"])
    : undefined;
  if (safeConfig) err.config = safeConfig;
  // No redaction makes a live ClientRequest safe, and nothing downstream needs it.
  if (err.request) delete err.request;
  if (err.response) {
    // Axios exposes the same objects twice. Clearing only the top-level
    // references leaves both the config and the raw ClientRequest — whose
    // `_header` is the literal bytes sent over the wire — reachable one level
    // deeper, where `util.inspect` finds them.
    if (safeConfig) {
      (err.response as { config?: unknown }).config = safeConfig;
    }
    if (err.response.request) delete err.response.request;
    if (err.response.headers) {
      err.response.headers = redactHeaders(
        err.response.headers as Record<string, unknown>,
        extraHeaderNames
      );
    }
    if (err.response.data !== undefined) {
      err.response.data = redactBody(err.response.data);
    }
    // Some upstreams echo the offending value into the reason phrase.
    if (typeof err.response.statusText === "string") {
      err.response.statusText = redactBody(err.response.statusText) as string;
    }
  }
  return error;
}
