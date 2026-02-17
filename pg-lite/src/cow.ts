import { PGlite } from '@electric-sql/pglite';
import {
  SETUP_COW_SQL,
  COMMIT_COW_OPERATIONS_SQL,
  DISCARD_COW_OPERATIONS_SQL,
  GET_COW_DEPENDENCIES_SQL,
} from './sql';
import type { CowDependency, CowTableConfig } from './types';

export interface CowManagerOptions {
  dataDir?: string;
  instance?: PGlite;
}

export class CowManager {
  private db: PGlite | null = null;
  private dataDir: string;
  private externalInstance: PGlite | null;
  private tables: Map<string, CowTableConfig> = new Map();
  private initialized = false;

  constructor(options: CowManagerOptions = {}) {
    this.dataDir = options.dataDir ?? 'idb://agent-cow';
    this.externalInstance = options.instance ?? null;
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    this.db = this.externalInstance ?? new PGlite(this.dataDir);
    if (!this.externalInstance) {
      await this.db.waitReady;
    }
    await this.deployFunctions();
    this.initialized = true;
  }

  private async deployFunctions(): Promise<void> {
    const db = this.getDb();
    await db.exec(SETUP_COW_SQL);
    await db.exec(COMMIT_COW_OPERATIONS_SQL);
    await db.exec(DISCARD_COW_OPERATIONS_SQL);
    await db.exec(GET_COW_DEPENDENCIES_SQL);
  }

  private getDb(): PGlite {
    if (!this.db) throw new Error('CowManager not initialized. Call init() first.');
    return this.db;
  }

  async setupTable(viewName: string, pkColumn: string): Promise<void> {
    const baseName = `${viewName}_base`;
    const db = this.getDb();
    const result = await db.query(`SELECT setup_cow($1, $2, $3)`, [baseName, viewName, pkColumn]);
    void result;
    this.tables.set(viewName, { baseName, viewName, pkColumn });
  }

  async setSession(sessionId: string, operationId?: string): Promise<void> {
    const db = this.getDb();
    await db.exec(`SET app.session_id = '${sessionId}'`);
    if (operationId) {
      await db.exec(`SET app.operation_id = '${operationId}'`);
    }
  }

  async clearSession(): Promise<void> {
    const db = this.getDb();
    await db.exec(`SET app.session_id = ''`);
    await db.exec(`SET app.operation_id = ''`);
  }

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    const db = this.getDb();
    const result = await db.query<T>(sql, params);
    return result.rows;
  }

  async exec(sql: string): Promise<void> {
    const db = this.getDb();
    await db.exec(sql);
  }

  async withSession<T>(
    sessionId: string,
    operationId: string | undefined,
    fn: () => Promise<T>
  ): Promise<T> {
    await this.setSession(sessionId, operationId);
    const result = await fn();
    await this.clearSession();
    return result;
  }

  async getChanges<T>(viewName: string, sessionId: string): Promise<T[]> {
    const db = this.getDb();
    const changesTable = `${viewName}_changes`;
    const result = await db.query<T>(
      `SELECT * FROM ${changesTable} WHERE session_id = $1 ORDER BY _cow_updated_at`,
      [sessionId]
    );
    return result.rows;
  }

  async commit(viewName: string, pkColumn: string, sessionId: string, operationIds: string[]): Promise<void> {
    if (operationIds.length === 0) return;
    const baseName = `${viewName}_base`;
    const db = this.getDb();
    await db.query(`SELECT commit_cow_operations($1, $2, $3, $4)`, [
      baseName, pkColumn, sessionId, operationIds,
    ]);
  }

  async commitAll(sessionId: string, operationIds: string[]): Promise<void> {
    if (operationIds.length === 0) return;
    for (const [, config] of this.tables) {
      await this.commit(config.viewName, config.pkColumn, sessionId, operationIds);
    }
  }

  async discard(viewName: string, sessionId: string, operationIds: string[]): Promise<void> {
    if (operationIds.length === 0) return;
    const baseName = `${viewName}_base`;
    const db = this.getDb();
    await db.query(`SELECT discard_cow_operations($1, $2, $3)`, [
      baseName, sessionId, operationIds,
    ]);
  }

  async discardAll(sessionId: string, operationIds: string[]): Promise<void> {
    if (operationIds.length === 0) return;
    for (const [, config] of this.tables) {
      await this.discard(config.viewName, sessionId, operationIds);
    }
  }

  async getDependencies(sessionId: string): Promise<CowDependency[]> {
    const db = this.getDb();
    const result = await db.query<CowDependency>(
      `SELECT depends_on, operation_id FROM get_cow_dependencies($1)`,
      [sessionId]
    );
    return result.rows;
  }

  getRegisteredTables(): CowTableConfig[] {
    return Array.from(this.tables.values());
  }

  async reset(): Promise<void> {
    if (this.db && !this.externalInstance) {
      await this.db.close();
      this.db = null;
    }

    if (!this.externalInstance && typeof indexedDB !== 'undefined') {
      const databases = await indexedDB.databases();
      for (const dbInfo of databases) {
        if (dbInfo.name?.startsWith(this.dataDir) || dbInfo.name?.includes(`/${this.dataDir}/`)) {
          indexedDB.deleteDatabase(dbInfo.name);
        }
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    this.tables.clear();
    this.initialized = false;
  }

  async close(): Promise<void> {
    if (this.db && !this.externalInstance) {
      await this.db.close();
    }
    this.db = null;
    this.initialized = false;
  }
}
