import type { BaseCredentialsData } from "../client/types.js";
import { ConfigurationError } from "../errors.js";

/**
 * Builds the canonical client name:
 * `<templateName>:<organizationId | "_">:<instanceId>[:<subClientPath>]`
 *
 * Positional, so a cross-service consumer can parse it with {@link parseClientName}
 * without knowing template specifics. See docs/concepts.md#client-names.
 */
export function buildClientName(
  templateName: string,
  creds: Pick<BaseCredentialsData, "instanceId" | "organizationId">
): string {
  assertNameSegment(creds.instanceId, "instanceId");
  if (creds.organizationId !== undefined && creds.organizationId !== null) {
    assertNameSegment(creds.organizationId, "organizationId");
  }
  return `${templateName}:${creds.organizationId ?? "_"}:${creds.instanceId}`;
}

/** The `organizationId` segment used when an integration is global. Reserved. */
export const GLOBAL_ORGANIZATION_SEGMENT = "_";

/**
 * Rejects a segment that would make a client name ambiguous.
 *
 * The name is `:`-delimited, positional, and doubles as the OAuth credential
 * cache key — so `org "a"` + `instance "b:c"` and `org "a:b"` + `instance "c"`
 * would resolve to one client holding one set of credentials. Whitespace is
 * rejected too, since namespaces normalize it to `_`.
 * `registerClientTemplate` applies the same rule to the template name.
 */
export function assertNameSegment(value: string, field: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new ConfigurationError(
      "invalid_client_name_segment",
      `${field} must be a non-empty string. Received ${JSON.stringify(value)}.`
    );
  }
  if (value.includes(":")) {
    throw new ConfigurationError(
      "invalid_client_name_segment",
      `${field} must not contain ":" — client names are colon-delimited, and a colon here lets two different tenants resolve to one client and one credential entry. Received ${JSON.stringify(
        value
      )}.`
    );
  }
  if (/\s/.test(value)) {
    throw new ConfigurationError(
      "invalid_client_name_segment",
      `${field} must not contain whitespace — it is normalized away when deriving backend keys, so two values differing only by a space would share one credential entry. Received ${JSON.stringify(
        value
      )}.`
    );
  }
  if (field === "organizationId" && value === GLOBAL_ORGANIZATION_SEGMENT) {
    throw new ConfigurationError(
      "invalid_client_name_segment",
      `organizationId must not be "${GLOBAL_ORGANIZATION_SEGMENT}" — that value is reserved to mean "no organization", so a tenant using it would collide with every global client sharing its instanceId.`
    );
  }
}

export interface ParsedClientName {
  templateName: string;
  organizationId: string | null;
  alias: string;
  subClientPath: string;
}

/**
 * Inverse of `buildClientName`. Returns `null` if the input has fewer than
 * three colon-separated segments — anything shorter isn't an alias-shaped
 * clientName (e.g., `default` for the handler's no-template default
 * client).
 */
export function parseClientName(clientName: string): ParsedClientName | null {
  const parts = clientName.split(":");
  if (parts.length < 3) return null;
  const [templateName, orgSegment, alias, ...rest] = parts;
  return {
    templateName,
    organizationId: orgSegment === "_" ? null : orgSegment,
    alias,
    subClientPath: rest.join(":"),
  };
}
