import { describe, expect, it } from "vitest";
import {
  buildClientName,
  parseClientName,
} from "../packages/core/src/utils/clientName.js";

describe("buildClientName", () => {
  it("uses `_` in the org position for a global integration", () => {
    expect(buildClientName("fedex", { instanceId: "production" })).toBe(
      "fedex:_:production"
    );
  });

  it("embeds the organization id when the integration is org-scoped", () => {
    expect(
      buildClientName("fedex", {
        instanceId: "production",
        organizationId: "01k1abc",
      })
    ).toBe("fedex:01k1abc:production");
  });

  it("treats an explicitly null organizationId as global", () => {
    expect(
      buildClientName("ups", { instanceId: "sandbox", organizationId: null })
    ).toBe("ups:_:sandbox");
  });
});

describe("parseClientName", () => {
  it("round-trips a global client name", () => {
    const name = buildClientName("fedex", { instanceId: "production" });
    expect(parseClientName(name)).toEqual({
      templateName: "fedex",
      organizationId: null,
      alias: "production",
      subClientPath: "",
    });
  });

  it("round-trips an org-scoped client name", () => {
    const name = buildClientName("fedex", {
      instanceId: "production",
      organizationId: "01k1abc",
    });
    expect(parseClientName(name)).toEqual({
      templateName: "fedex",
      organizationId: "01k1abc",
      alias: "production",
      subClientPath: "",
    });
  });

  it("keeps a multi-segment sub-client path intact", () => {
    // Amazon SP-API names carry region + marketplace as nested sub-clients.
    const parsed = parseClientName(
      "amazonSpapi:01k1abc:acme:us-east-1:ATVPDKIKX0DER"
    );
    expect(parsed).toEqual({
      templateName: "amazonSpapi",
      organizationId: "01k1abc",
      alias: "acme",
      subClientPath: "us-east-1:ATVPDKIKX0DER",
    });
  });

  it("returns null for names that are not alias-shaped", () => {
    // The handler's own default client has no template behind it.
    expect(parseClientName("default")).toBeNull();
    expect(parseClientName("fedex:_")).toBeNull();
  });
});
