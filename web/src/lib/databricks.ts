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

export async function dbQuery<T = Record<string, unknown>>(
  sql: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  params: any[] = []
): Promise<T[]> {
  const { client, session } = await openSession();
  try {
    const operation = await session.executeStatement(sql, {
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
