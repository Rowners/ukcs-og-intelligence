/**
 * Server-side Databricks query helper.
 * Only import from API routes or server components.
 */
import { DBSQLClient } from "@databricks/sql";

function getClient() {
  return new DBSQLClient();
}

const REQUIRED_ENV = [
  "DATABRICKS_SERVER_HOSTNAME",
  "DATABRICKS_HTTP_PATH",
  "DATABRICKS_ACCESS_TOKEN",
] as const;

/**
 * Netlify Functions expose site env through Netlify.env. Next.js dev and
 * `next build` only have process.env, so fall back when the Netlify global
 * is absent.
 */
function readEnv(name: string): string | undefined {
  const netlify = (globalThis as { Netlify?: { env?: { get?: (key: string) => string | undefined } } })
    .Netlify?.env;
  const fromNetlify = netlify?.get?.(name);
  if (typeof fromNetlify === "string" && fromNetlify.length > 0) return fromNetlify;
  const fromProcess = process.env[name];
  return fromProcess && fromProcess.length > 0 ? fromProcess : undefined;
}

export function missingDatabricksEnv(): string[] {
  return REQUIRED_ENV.filter((name) => !readEnv(name));
}

/** Short message safe to show in the UI. Tokens are stripped. */
export function describeDbError(err: unknown): string {
  const missing = missingDatabricksEnv();
  if (missing.length > 0) {
    return `Missing environment variables: ${missing.join(", ")}`;
  }
  const message = err instanceof Error ? err.message : String(err);
  return message.replace(/dapi[A-Za-z0-9._\-]+/g, "[token]").slice(0, 500);
}

async function openSession() {
  const missing = missingDatabricksEnv();
  if (missing.length > 0) {
    throw new Error(`Missing environment variables: ${missing.join(", ")}`);
  }
  const client = getClient();
  await client.connect({
    host: readEnv("DATABRICKS_SERVER_HOSTNAME")!,
    path: readEnv("DATABRICKS_HTTP_PATH")!,
    token: readEnv("DATABRICKS_ACCESS_TOKEN")!,
    // Milliseconds. Netlify synchronous functions stop at 60s and that limit
    // is not configurable, so fail the query well inside it.
    socketTimeout: 20_000,
  });
  const session = await client.openSession();
  return { client, session };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function bindParams(sql: string, params: any[]): string {
  let i = 0;
  return sql.replace(/\?/g, () => {
    const val = params[i++];
    if (val === null || val === undefined) return "NULL";
    if (typeof val === "number") return String(val);
    return `'${String(val).replace(/'/g, "''")}'`;
  });
}

export async function dbQuery<T = Record<string, unknown>>(
  sql: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  params: any[] = []
): Promise<T[]> {
  const boundSql = params.length > 0 ? bindParams(sql, params) : sql;
  const { client, session } = await openSession();
  try {
    const operation = await session.executeStatement(boundSql, {
      runAsync: true,
      queryTimeout: BigInt(60),
    });
    const result = await operation.fetchAll();
    await operation.close();
    return result as T[];
  } finally {
    await session.close();
    await client.close();
  }
}
