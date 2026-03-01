/**
 * Server-side Databricks query helper.
 * Only import from API routes or server components.
 */
import { DBSQLClient } from "@databricks/sql";

function getClient() {
  return new DBSQLClient();
}

async function openSession() {
  const client = getClient();
  await client.connect({
    host: process.env.DATABRICKS_SERVER_HOSTNAME!,
    path: process.env.DATABRICKS_HTTP_PATH!,
    token: process.env.DATABRICKS_ACCESS_TOKEN!,
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
