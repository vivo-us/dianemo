import { SCRIPTS } from "../packages/backend-redis/src/lua/index.js";
import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { fileURLToPath } from "node:url";

/**
 * Static guards on the Lua argument contract.
 *
 * A script's arguments are positional and untyped: ioredis splits a call into
 * keys and ARGV on the registered key count alone, so a miscount or a renumbered
 * slot yields a script that still parses, still runs and still succeeds while
 * reading the wrong value. None of that is observable from behaviour, which is
 * why these assert over the script text rather than through Redis.
 *
 * What they catch: a `keys` count that disagrees with the `KEYS[n]` the body
 * reads, a gap left by a removed argument, a call site passing the wrong number
 * of arguments, and a script that is registered but never called or called but
 * never registered.
 *
 * What they do NOT catch: two arguments of the same arity swapped for each other.
 * Every count stays identical, so the transposed-TTL hazard described in
 * `lua/concurrency.ts` survives all six assertions — that one still needs the
 * direct check that comment prescribes.
 */

const OPS_DIR = fileURLToPath(
  new URL("../packages/backend-redis/src/ops/", import.meta.url)
);

/** Lua with its comments removed, so prose cannot be mistaken for code. */
function luaCode(lua: string): string {
  return lua
    .split("\n")
    .map((line) => line.replace(/--.*$/, ""))
    .join("\n");
}

/** Indices of `KEYS[n]` / `ARGV[n]` actually read, comments excluded. */
function readIndices(lua: string, token: "KEYS" | "ARGV"): number[] {
  const matches = luaCode(lua).matchAll(
    new RegExp(`\\b${token}\\[(\\d+)\\]`, "g")
  );
  return [...new Set([...matches].map((m) => Number(m[1])))].sort(
    (a, b) => a - b
  );
}

/**
 * TypeScript with its comments removed, tracking string and template literals so
 * a `//` inside a quoted string is not mistaken for a comment.
 *
 * `callSites` below counts commas, and a comment is the one place a comma can
 * appear that is neither an argument separator nor inside a literal it tracks.
 * Three things went wrong without this: `86400 // TTL, in seconds` counted an
 * argument that is not passed, a commented-out `ctx.run(...)` registered as a
 * live call, and an apostrophe in prose — `the key's own TTL` — opened a string
 * that ran past the closing paren and swallowed the next call whole.
 */
function stripComments(source: string): string {
  let out = "";
  let quote: string | null = null;

  for (let i = 0; i < source.length; i++) {
    const c = source[i];

    if (quote) {
      out += c;
      if (c === "\\") {
        out += source[i + 1] ?? "";
        i++;
      } else if (c === quote) quote = null;
      continue;
    }
    if (c === '"' || c === "'" || c === "`") {
      quote = c;
      out += c;
      continue;
    }
    if (c === "/" && source[i + 1] === "/") {
      while (i < source.length && source[i] !== "\n") i++;
      out += "\n";
      continue;
    }
    if (c === "/" && source[i + 1] === "*") {
      const end = source.indexOf("*/", i + 2);
      i = end === -1 ? source.length : end + 1;
      continue;
    }
    out += c;
  }

  return out;
}

interface CallSite {
  file: string;
  script: string;
  /** Arguments after the command name. */
  count: number;
  /** Spread arguments, which supply a trailing run the script reads via `#ARGV`. */
  spreads: number;
}

/**
 * Counts the arguments at each `ctx.run("dianemoX", ...)`.
 *
 * Depth-tracking rather than a regex: the calls span lines and contain nested
 * calls, template literals and object literals, all of which carry commas that
 * are not argument separators.
 */
function callSites(source: string, file: string): CallSite[] {
  const sites: CallSite[] = [];
  const marker = "ctx.run(";

  for (let at = source.indexOf(marker); at !== -1;) {
    let i = at + marker.length;
    let depth = 0;
    let quote: string | null = null;
    // Split on top-level commas and keep the segments, rather than counting the
    // commas: a trailing comma before `)` would otherwise read as one argument
    // more than the call actually passes.
    const segments: string[] = [];
    let current = "";

    for (; i < source.length; i++) {
      const c = source[i];

      if (quote) {
        current += c;
        if (c === "\\") {
          current += source[i + 1] ?? "";
          i++;
        } else if (c === quote) quote = null;
        continue;
      }
      if (c === '"' || c === "'" || c === "`") {
        quote = c;
        current += c;
        continue;
      }
      if (c === "(" || c === "[" || c === "{") depth++;
      else if (c === ")" && depth === 0) break;
      else if (c === ")" || c === "]" || c === "}") depth--;

      if (c === "," && depth === 0) {
        segments.push(current);
        current = "";
      } else {
        current += c;
      }
    }
    segments.push(current);

    const args = segments.map((s) => s.trim()).filter((s) => s !== "");
    const name = /^"([^"]+)"$/.exec(args[0] ?? "");
    if (name) {
      sites.push({
        file,
        script: name[1],
        count: args.length - 1,
        spreads: args.filter((a) => a.startsWith("...")).length,
      });
    }
    at = source.indexOf(marker, i);
  }

  return sites;
}

const scripts = Object.entries(SCRIPTS);

const sites = readdirSync(OPS_DIR)
  .filter((f) => f.endsWith(".ts"))
  .flatMap((f) =>
    callSites(stripComments(readFileSync(OPS_DIR + f, "utf8")), f)
  );

describe("lua argument contract", () => {
  it("registers each script under the highest key index it reads", () => {
    const wrong = scripts
      .map(([name, { keys, lua }]) => {
        const read = readIndices(lua, "KEYS");
        const highest = read.length ? Math.max(...read) : 0;
        return { name, keys, highest };
      })
      .filter(({ keys, highest }) => keys !== highest);

    expect(wrong).toEqual([]);
  });

  it("reads every key it registers, with no gaps and none beyond the count", () => {
    const wrong = scripts
      .map(([name, { keys, lua }]) => {
        const read = readIndices(lua, "KEYS");
        const missing = Array.from({ length: keys }, (_, i) => i + 1).filter(
          (n) => !read.includes(n)
        );
        const beyond = read.filter((n) => n > keys);
        return { name, missing, beyond };
      })
      .filter(({ missing, beyond }) => missing.length > 0 || beyond.length > 0);

    expect(wrong).toEqual([]);
  });

  it("numbers ARGV contiguously from 1, so a removed argument cannot leave a hole", () => {
    const wrong = scripts
      .map(([name, { lua }]) => {
        const read = readIndices(lua, "ARGV");
        // A script iterating `#ARGV` reads a trailing run this cannot see. Tested
        // against comment-stripped Lua, or merely mentioning `#ARGV` in prose
        // would exempt a script from the check.
        if (!read.length || /#ARGV/.test(luaCode(lua)))
          return { name, gaps: [] };
        const gaps = Array.from(
          { length: Math.max(...read) },
          (_, i) => i + 1
        ).filter((n) => !read.includes(n));
        return { name, gaps };
      })
      .filter(({ gaps }) => gaps.length > 0);

    expect(wrong).toEqual([]);
  });

  it("passes exactly as many fixed arguments as each script registers and reads", () => {
    // A spread stands in for the trailing run read via `#ARGV`, so it is not one
    // of the positional slots the script names.
    const expected = new Map(
      scripts.map(([name, { keys, lua }]) => {
        const argv = readIndices(lua, "ARGV");
        return [name, keys + (argv.length ? Math.max(...argv) : 0)];
      })
    );

    const wrong = sites
      .map((site) => ({
        ...site,
        fixed: site.count - site.spreads,
        expected: expected.get(site.script),
      }))
      .filter(
        (site) => site.expected !== undefined && site.fixed !== site.expected
      );

    expect(wrong).toEqual([]);
  });

  it("calls only scripts that are registered", () => {
    const registered = new Set(scripts.map(([name]) => name));
    const unknown = sites.filter((s) => !registered.has(s.script));

    expect(unknown).toEqual([]);
  });

  it("calls every script it registers, so a dead script cannot linger", () => {
    const called = new Set(sites.map((s) => s.script));
    const uncalled = scripts
      .map(([name]) => name)
      .filter((n) => !called.has(n));

    expect(uncalled).toEqual([]);
  });
});

/**
 * The assertions above are only worth their runtime if the parser under them is
 * honest, and every case here is one it got wrong before: each produced a green
 * suite over a call that was actually malformed.
 */
describe("the call-site parser itself", () => {
  const parse = (source: string) =>
    callSites(stripComments(source), "probe.ts");

  it("does not count a comma inside a trailing comment as an argument", () => {
    const one = parse(
      `ctx.run("dianemoTokenBucket", key, cost, a, b, 86400 // TTL, in seconds\n);`
    );
    expect(one[0].count).toBe(5);
  });

  it("does not treat a commented-out call as a live call site", () => {
    expect(parse(`// await ctx.run("dianemoTokenBucket", key, cost);`)).toEqual(
      []
    );
    expect(parse(`/* ctx.run("dianemoTokenBucket", key, cost); */`)).toEqual(
      []
    );
  });

  it("does not let an apostrophe in prose swallow the following call", () => {
    const two = parse(
      `ctx.run("dianemoTokenBucket", key, cost, a, b, 86400 // the key's own TTL\n);\n` +
        `await ctx.run("dianemoTryAdmitNoLimit", queueKey, freezeKey);`
    );
    expect(two.map((s) => s.script)).toEqual([
      "dianemoTokenBucket",
      "dianemoTryAdmitNoLimit",
    ]);
  });

  it("keeps a `//` that is inside a string literal rather than a comment", () => {
    const one = parse(`ctx.run("dianemoTokenBucket", "http://x", b, c, d, e);`);
    expect(one[0].count).toBe(5);
  });

  it("counts a trailing comma as no argument at all", () => {
    const one = parse(
      `ctx.run("dianemoTryAdmitNoLimit", queueKey, freezeKey,);`
    );
    expect(one[0].count).toBe(2);
  });
});
