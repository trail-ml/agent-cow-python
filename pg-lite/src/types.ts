export interface CowDependency {
  depends_on: string;
  operation_id: string;
}

export interface CowTableConfig {
  baseName: string;
  viewName: string;
  pkColumn: string;
}

export interface CowSessionContext {
  sessionId: string;
  operationId?: string;
}
