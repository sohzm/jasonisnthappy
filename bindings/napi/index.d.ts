/**
 * TypeScript type definitions for jasonisnthappy
 */

// =============================================================================
// Base Document Type
// =============================================================================

/** Base type for all documents - must have _id after insertion */
export interface Document {
  _id: string;
  [key: string]: unknown;
}

// =============================================================================
// Configuration Types
// =============================================================================

export interface DatabaseOptions {
  cacheSize?: number;
  autoCheckpointThreshold?: number;
  filePermissions?: number;
  readOnly?: boolean;
  maxBulkOperations?: number;
  maxDocumentSize?: number;
  maxRequestBodySize?: number;
}

export interface TransactionConfig {
  maxRetries?: number;
  retryBackoffBaseMs?: number;
  maxRetryBackoffMs?: number;
}

// =============================================================================
// Result Types
// =============================================================================

export interface UpsertResult {
  id: string;
  inserted: boolean;
}

export interface BulkWriteResult {
  inserted_count: number;
  updated_count: number;
  deleted_count: number;
  inserted_ids: string[];
  errors: BulkWriteError[];
}

export interface BulkWriteError {
  operation_index: number;
  message: string;
}

export interface SearchResult {
  doc_id: string;
  score: number;
}

export interface CollectionInfo {
  name: string;
  document_count: number;
  btree_root: number;
  indexes: IndexInfo[];
}

export interface IndexInfo {
  name: string;
  fields: string[];
  unique: boolean;
  index_type: 'btree' | 'text';
}

export interface DatabaseInfo {
  path: string;
  version: number;
  num_pages: number;
  file_size: number;
  collections: CollectionInfo[];
  total_documents: number;
  read_only: boolean;
}

export interface GarbageCollectResult {
  pages_freed: number;
  bytes_freed: number;
}

export interface BackupInfo {
  path: string;
  version: number;
  collections: string[];
  total_documents: number;
}

export interface MetricsSnapshot {
  documents_written: number;
  documents_read: number;
  total_document_operations: number;
  io_errors: number;
  transaction_conflicts: number;
}

// =============================================================================
// Operation Types
// =============================================================================

export interface BulkInsertOp<T> {
  op: 'insert';
  doc: T;
}

export interface BulkUpdateOneOp {
  op: 'update_one';
  filter: string;
  update: Record<string, unknown>;
}

export interface BulkUpdateManyOp {
  op: 'update_many';
  filter: string;
  update: Record<string, unknown>;
}

export interface BulkDeleteOneOp {
  op: 'delete_one';
  filter: string;
}

export interface BulkDeleteManyOp {
  op: 'delete_many';
  filter: string;
}

export type BulkOperation<T = Record<string, unknown>> =
  | BulkInsertOp<T>
  | BulkUpdateOneOp
  | BulkUpdateManyOp
  | BulkDeleteOneOp
  | BulkDeleteManyOp;

export interface AggregationStage {
  match?: string;
  group_by?: string;
  count?: string;
  sum?: { field: string; output: string };
  avg?: { field: string; output: string };
  min?: { field: string; output: string };
  max?: { field: string; output: string };
  sort?: { field: string; asc?: boolean };
  limit?: number;
  skip?: number;
  project?: string[];
  exclude?: string[];
}

// =============================================================================
// Watch Types
// =============================================================================

export type ChangeOperation = 'insert' | 'update' | 'delete';

export type WatchCallback<T> = (
  operation: ChangeOperation,
  docId: string,
  document: T | null
) => void;

// =============================================================================
// Database Class
// =============================================================================

export class Database {
  static open(path: string): Database;
  static openWithOptions(path: string, options: DatabaseOptions): Database;
  static defaultDatabaseOptions(): DatabaseOptions;
  static verifyBackup(backupPath: string): BackupInfo;

  close(): void;

  // Configuration
  defaultTransactionConfig(): TransactionConfig;
  setTransactionConfig(config: TransactionConfig): void;
  getTransactionConfig(): TransactionConfig;
  setAutoCheckpointThreshold(threshold: number): void;

  // Info
  getPath(): string;
  isReadOnly(): boolean;
  maxBulkOperations(): number;
  maxDocumentSize(): number;
  maxRequestBodySize(): number;
  listCollections(): string[];
  collectionStats(collectionName: string): CollectionInfo;
  databaseInfo(): DatabaseInfo;

  // Index Management
  listIndexes(collectionName: string): IndexInfo[];
  createIndex(collectionName: string, indexName: string, field: string, unique: boolean): void;
  createCompoundIndex(collectionName: string, indexName: string, fields: string[], unique: boolean): void;
  createTextIndex(collectionName: string, indexName: string, field: string): void;
  dropIndex(collectionName: string, indexName: string): void;

  // Schema
  setSchema(collectionName: string, schema: Record<string, unknown>): void;
  getSchema(collectionName: string): Record<string, unknown> | null;
  removeSchema(collectionName: string): void;

  // Maintenance
  checkpoint(): void;
  backup(destPath: string): void;
  garbageCollect(): GarbageCollectResult;
  metrics(): MetricsSnapshot;
  frameCount(): number;

  // Collections & Transactions
  beginTransaction(): Transaction;
  getCollection<T extends Document>(name: string): Collection<T>;
  startWebUi(addr: string): WebServer;
}

// =============================================================================
// WebServer Class
// =============================================================================

export class WebServer {
  stop(): void;
}

// =============================================================================
// Transaction Class
// =============================================================================

export class Transaction {
  isActive(): boolean;
  commit(): void;
  rollback(): void;

  // CRUD - use generics for type safety
  insert<T extends Document>(collectionName: string, doc: Omit<T, '_id'>): string;
  findById<T extends Document>(collectionName: string, id: string): T | null;
  updateById<T extends Document>(collectionName: string, id: string, updates: Partial<T>): void;
  deleteById(collectionName: string, id: string): void;
  findAll<T extends Document>(collectionName: string): T[];
  count(collectionName: string): number;

  // Collection Management
  createCollection(collectionName: string): void;
  dropCollection(collectionName: string): void;
  renameCollection(oldName: string, newName: string): void;
}

// =============================================================================
// Collection Class
// =============================================================================

export class Collection<T extends Document> {
  name(): string;

  // Basic CRUD
  insert(doc: Omit<T, '_id'>): string;
  findById(id: string): T | null;
  updateById(id: string, updates: Partial<T>): void;
  deleteById(id: string): void;
  findAll(): T[];
  count(): number;

  // Query Operations
  find(filter: string): T[];
  findOne(filter: string): T | null;
  update(filter: string, updates: Partial<T>): number;
  updateOne(filter: string, updates: Partial<T>): boolean;
  delete(filter: string): number;
  deleteOne(filter: string): boolean;

  // Upsert
  upsertById(id: string, doc: Omit<T, '_id'>): UpsertResult;
  upsert(filter: string, doc: Omit<T, '_id'>): UpsertResult;

  // Bulk Operations
  insertMany(docs: Omit<T, '_id'>[]): string[];
  bulkWrite(operations: BulkOperation<Omit<T, '_id'>>[], ordered?: boolean): BulkWriteResult;

  // Advanced Queries
  distinct<K extends keyof T>(field: K): T[K][];
  countDistinct(field: keyof T): number;
  search(query: string): SearchResult[];
  countWithQuery(filter?: string): number;

  // Query Builder (flat)
  queryWithOptions(
    filter?: string,
    sortField?: keyof T & string,
    sortAsc?: boolean,
    limit?: number,
    skip?: number,
    projectFields?: (keyof T & string)[],
    excludeFields?: (keyof T & string)[]
  ): T[];
  queryCount(filter?: string, skip?: number, limit?: number): number;
  queryFirst(filter?: string, sortField?: keyof T & string, sortAsc?: boolean): T | null;

  // Aggregation - returns different shape, so use separate generic
  aggregate<R>(pipeline: AggregationStage[]): R[];

  // Watch
  watch(filter: string | undefined, callback: WatchCallback<T>): WatchHandle;
}

// =============================================================================
// WatchHandle Class
// =============================================================================

export class WatchHandle {
  stop(): void;
}
