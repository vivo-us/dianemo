/**
 * The logging surface the handler needs, narrowed to four levels and Pino's
 * two call shapes — `log(obj, msg)` and `log(msg)`.
 *
 * A `pino` instance satisfies this structurally, so the common case is
 * `new RequestHandler({ logger: pino() })`. Anything else with these four
 * methods works too, including `console`.
 */
export interface LogFn {
  (obj: unknown, msg?: string, ...args: unknown[]): void;
  (msg: string, ...args: unknown[]): void;
}

export interface Logger {
  debug: LogFn;
  info: LogFn;
  warn: LogFn;
  error: LogFn;
}

/**
 * Default when no logger is injected. Silence is the right default for a
 * library: a handler embedded in someone else's process should not write to
 * their stdout uninvited.
 */
export const noopLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};
