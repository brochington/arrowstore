import type { ColumnSchema, TableSchema } from './types';

/**
 * Abstract base DataStore class
 * T = row data type (e.g., Record<string, any>)
 * S = source data type (e.g., Array, Table, etc.)
 */
export abstract class DataStore<T extends Record<string, any>, S = any> {
  protected _schema: TableSchema;
  protected _isLazy: boolean;

  constructor(schema: TableSchema, isLazy = false) {
    this._schema = schema;
    this._isLazy = isLazy;
  }

  get isLazy(): boolean {
    return this._isLazy;
  }

  getSchema(): TableSchema {
    return { ...this._schema };
  }

  setSchema(schema: TableSchema): void {
    this._schema = schema;
  }

  getColumnSchema(columnName: string): ColumnSchema | undefined {
    return this._schema.columns.find((col) => col.name === columnName);
  }

  getColumns(): ColumnSchema[] {
    return [...this._schema.columns];
  }

  /**
   * Get the raw source data object
   */
  abstract getSource(): S;

  /**
   * Convert source data to array of row objects
   */
  abstract getAll(): Promise<T[]>;

  /**
   * Count the number of rows
   */
  abstract count(): Promise<number>;

  /**
   * Create a new instance with a transformed source
   */
  protected abstract createInstance<R extends Record<string, any>, U = any>(
    executor: (source: S) => Promise<U>,
    isLazy?: boolean,
  ): DataStore<R, S>;
}
