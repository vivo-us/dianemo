import { SCRIPTS, type ScriptName } from "./lua/index.js";
import type { Redis } from "ioredis";

/**
 * What every operation module needs from the connection, and nothing more.
 *
 * Passing this rather than the backend itself is what lets the operations live
 * in one module per domain instead of one class with every method on it.
 */
export interface Ctx {
  readonly redis: Redis;
  /** Invokes a registered Lua script by its custom-command name. */
  run(name: ScriptName, ...args: unknown[]): Promise<unknown>;
  /** Redis's clock in epoch milliseconds. */
  now(): Promise<number>;
}

/**
 * Registers every Lua script as an ioredis custom command.
 *
 * `eval` ships the whole script body on every call — kilobytes across the several
 * scripts one request touches, which at a few thousand requests a second is
 * megabytes a second of pure script text. A custom command sends EVALSHA instead,
 * falling back to EVAL once if Redis reports NOSCRIPT.
 */
function defineCommands(redis: Redis): void {
  for (const [name, { keys, lua }] of Object.entries(SCRIPTS)) {
    // Skip if a sibling backend on the same connection already defined it.
    if ((redis as unknown as Record<string, unknown>)[name]) continue;
    redis.defineCommand(name, { numberOfKeys: keys, lua });
  }
}

/** Registers the scripts on `redis` and returns the handle the operations use. */
export function createContext(redis: Redis): Ctx {
  defineCommands(redis);

  return {
    redis,

    run(name, ...args) {
      // ioredis attaches the command to the connection, but its generated
      // typings are not visible here, so the lookup is dynamic and typed at
      // this boundary.
      const fn = (redis as unknown as Record<string, unknown>)[name];
      if (typeof fn !== "function") {
        throw new Error(
          `Lua command "${name}" was not registered on this connection`
        );
      }
      return (fn as (...a: unknown[]) => Promise<unknown>).apply(redis, args);
    },

    async now() {
      const [seconds, micros] = await redis.time();
      return Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
    },
  };
}
