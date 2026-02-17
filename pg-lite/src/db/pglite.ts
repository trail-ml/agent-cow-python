import { PGlite } from '@electric-sql/pglite';

let db: PGlite | null = null;

export async function getDb(): Promise<PGlite> {
  if (!db) {
    db = new PGlite('idb://cow-demo');
    await db.waitReady;
  }
  return db;
}

export async function resetDb(): Promise<PGlite> {
  if (db) {
    await db.close();
    db = null;
  }

  const databases = await indexedDB.databases();
  for (const dbInfo of databases) {
    if (dbInfo.name?.startsWith('cow-demo') || dbInfo.name?.includes('/cow-demo/')) {
      indexedDB.deleteDatabase(dbInfo.name);
    }
  }

  await new Promise(resolve => setTimeout(resolve, 100));

  db = new PGlite('idb://cow-demo');
  await db.waitReady;
  return db;
}

export async function query<T>(sql: string, params?: unknown[]): Promise<T[]> {
  const database = await getDb();
  const result = await database.query<T>(sql, params);
  return result.rows;
}

export async function exec(sql: string): Promise<void> {
  const database = await getDb();
  await database.exec(sql);
}
