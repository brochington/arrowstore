import {
  RecordBatch,
  type StructRowProxy,
  Table,
  type Vector,
  makeTable,
  tableFromArrays,
} from 'apache-arrow';
import { DataStore } from './Datastore';
import type * as filters from './filter-helpers';
import {
  type SerializedFilterCondition,
  deserializeFilters,
  filtersFromJson,
  filtersToJson,
} from './filter-serialization';
import { parseSqlFilter } from './filter-sql';
import {
  arrayToColumnarFormat,
  capitalizeFirstLetter,
  compare,
  estimateMemoryUsage,
  inferSchemaFromTransformedData,
  mapArrowTypeToString,
} from './helpers';
import {
  type PivotOptions,
  type UnpivotOptions,
  pivot,
  unpivot,
} from './pivot';
import type { ColumnSchema, TableSchema } from './types';

type SortOptions = {
  field: string;
  direction: 'asc' | 'desc';
}[];

export type ArrowStoreOptions = {};

/**
 * ArrowStore implementation using Apache Arrow Table with vectorized operations
 * for improved performance - always using lazy evaluation
 */
export class ArrowStore<T extends Record<string, any>> extends DataStore<
  T,
  Table
> {
  #table: Table;
  #pendingOperations: ((
    table: Table,
    schema: TableSchema,
  ) => Promise<{ table: Table; schema?: TableSchema }>)[] = [];

  constructor(
    table: Table,
    schema?: TableSchema,
    _options?: ArrowStoreOptions,
  ) {
    // If schema is not provided, map Arrow schema to our TableSchema format
    const tableSchema = schema || {
      tableName: 'duckdbstore',
      columns: table.schema.fields.map((f) => ({
        name: f.name,
        type: mapArrowTypeToString(f.type),
        nullable: f.nullable,
      })),
    };

    // Always set isLazy to true
    super(tableSchema, true);
    this.#table = table;
  }

  get table(): Table {
    return this.#table;
  }

  /**
   * Get the underlying Arrow Table
   */
  getSource(): Table {
    return this.#table;
  }

  /**
   * Get Columns from Arrow Schema
   */

  getColumns(): ColumnSchema[] {
    return this._schema.columns;
  }

  /**
   * Get all data from the Arrow table
   */
  async getAll(): Promise<StructRowProxy[]> {
    try {
      const resolvedTable = await this.resolveTable();
      return resolvedTable.toArray();
    } catch (error) {
      // console.error('Error getting all data:', error);
      throw error;
    }
  }

  /**
   * Count the number of rows in the table
   */
  async count(): Promise<number> {
    const resolvedTable = await this.resolveTable();
    // Use Arrow's numRows property
    return resolvedTable.numRows;
  }

  /**
   * Explicitly execute all pending operations and return a new
   * ArrowStore with the resulting table
   */
  async flush(): Promise<ArrowStore<T>> {
    const resolvedTable = await this.resolveTable();
    return new ArrowStore<T>(resolvedTable, this._schema);
  }

  /**
   * Resolve all pending operations and return the resulting table
   * @param forceFlush When true, actually executes all operations
   */
  async resolveTable(): Promise<Table> {
    if (this.#pendingOperations.length === 0) {
      return this.#table;
    }

    let currentTable = this.#table;
    let resolvedSchema = this._schema;

    try {
      // Execute operations in batches to avoid stack overflow
      const batchSize = 10;
      for (let i = 0; i < this.#pendingOperations.length; i += batchSize) {
        const batch = this.#pendingOperations.slice(i, i + batchSize);

        // Execute operations in sequence
        for (const operation of batch) {
          const result = await operation(currentTable, resolvedSchema);
          currentTable = result.table;

          if (result.schema) {
            this._schema = result.schema;
            resolvedSchema = result.schema;
          }
        }

        // Allow GC to reclaim memory between batches
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    } catch (error) {
      // console.error('Error executing operations:', error);
      throw error;
    }

    // Create a new instance with the resolved table and reset pending operations
    this.#pendingOperations = [];
    this.#table = currentTable;
    this._schema = resolvedSchema;

    return currentTable;
  }

  /**
   * Create a new instance with the given schema and executor
   * With smart operation fusion and memory management
   */
  protected createInstance<
    R extends Record<string, any>,
    U = { table: Table<any>; schema?: TableSchema },
  >(executor: (source: Table<any>) => Promise<U>): ArrowStore<R> {
    const newArrowStore = new ArrowStore<R>(this.#table, this._schema);

    // Add operations to the queue and attempt simple operation fusion
    // For example, consecutive select operations could be combined
    const newOp = async (
      table: Table<any>,
      schema: TableSchema,
    ): Promise<{ table: Table<any>; schema?: TableSchema }> => {
      try {
        this._schema = schema;
        const result = await executor(table);
        return result as unknown as { table: Table<any>; schema?: TableSchema };
      } catch (error) {
        throw error;
      }
    };

    newArrowStore.#pendingOperations = [...this.#pendingOperations, newOp];

    return newArrowStore;
  }

  // Add memory usage estimation
  async estimateMemoryUsage(): Promise<{
    // Table memory details
    tableBytes: number;
    columnarDataBytes: number;
    metadataBytes: number;

    // Operation details
    pendingOperations: number;
    estimatedOperationOverhead: number;

    // Summary
    totalEstimatedBytes: number;
    humanReadable: {
      total: string;
      table: string;
      columnarData: string;
      metadata: string;
      operations: string;
    };
  }> {
    return estimateMemoryUsage(this.#table, this.#pendingOperations.length);
  }

  // Method to optimize memory usage
  async optimizeMemory(): Promise<void> {
    if (this.#pendingOperations.length > 0) {
      await this.flush();
    }

    // Force garbage collection if available (only in some JS environments)
    if (typeof global !== 'undefined' && global.gc) {
      global.gc();
    }
  }

  /**
   * Process batches of data for large tables to avoid memory issues
   * @param table Input Arrow table
   * @param batchSize Number of rows per batch
   * @param processFn Function to process each batch
   * @returns New Arrow table with processed data
   */
  private async processBatches<R extends Record<string, any>>(
    table: Table,
    batchSize: number,
    processFn: (batch: any[]) => R[],
  ): Promise<Table> {
    // For small tables, process all at once
    if (table.numRows <= batchSize) {
      const data = table.toArray();
      const processed = processFn(data);
      return tableFromArrays(arrayToColumnarFormat(processed));
    }

    // For large tables, process in batches
    const results: R[] = [];

    for (let i = 0; i < table.numRows; i += batchSize) {
      // Get a slice of the table
      const batchTable = table.slice(i, Math.min(i + batchSize, table.numRows));

      // Convert to array (only for this batch) and process
      const batchData = batchTable.toArray();
      const processedBatch = processFn(batchData);

      // Collect results
      results.push(...processedBatch);
    }

    // Convert results back to a table
    return tableFromArrays(arrayToColumnarFormat(results));
  }

  private validateField(fieldName: string): void {
    // Get all field names from the schema
    const fieldNames = this._schema.columns.map((col) => col.name);

    // Check if the field exists in the schema
    if (!fieldNames.includes(fieldName)) {
      throw new Error(
        `Filter error: Field '${fieldName}' does not exist in the table schema. Available fields are: ${fieldNames.join(
          ', ',
        )}`,
      );
    }
  }

  /**
   * Create a filter mask for complex filter conditions
   */
  private createComplexFilterMask(
    table: Table,
    filters: filters.Filters<T>,
  ): Uint8Array {
    const numRows = table.numRows;
    const mask = new Uint8Array(numRows).fill(1);

    // If no filters, return mask with all rows
    if (!filters || filters.length === 0) {
      return mask;
    }

    // Evaluate all top-level filter conditions (implicitly AND-ed together)
    for (const condition of filters) {
      this.evaluateFilterCondition(table, condition, mask);
    }

    return mask;
  }

  /**
   * Evaluate a single filter condition recursively
   */
  private evaluateFilterCondition(
    table: Table,
    condition: filters.FilterCondition<T>,
    mask: Uint8Array,
  ): void {
    // AND condition
    if ('AND' in condition) {
      // Create a temporary mask for each sub-condition
      for (const subCondition of condition.AND) {
        this.evaluateFilterCondition(table, subCondition, mask);
      }
      return;
    }

    // OR condition
    if ('OR' in condition) {
      // Create a new mask for OR conditions
      const orMask = new Uint8Array(mask.length).fill(0);

      // Evaluate each sub-condition
      for (const subCondition of condition.OR) {
        // Create a temporary mask for this sub-condition
        const tempMask = new Uint8Array(mask.length).fill(1);
        this.evaluateFilterCondition(table, subCondition, tempMask);

        // Combine with OR
        for (let i = 0; i < orMask.length; i++) {
          orMask[i] = orMask[i] || tempMask[i];
        }
      }

      // Apply the OR mask to the main mask (AND with the main mask)
      for (let i = 0; i < mask.length; i++) {
        mask[i] = mask[i] & orMask[i];
      }
      return;
    }

    // NOT condition
    if ('NOT' in condition) {
      // Create a temporary mask for the negated condition
      const notMask = new Uint8Array(mask.length).fill(1);
      this.evaluateFilterCondition(table, condition.NOT, notMask);

      // Negate the mask
      for (let i = 0; i < notMask.length; i++) {
        notMask[i] = notMask[i] ? 0 : 1;
      }

      // Apply to the main mask
      for (let i = 0; i < mask.length; i++) {
        mask[i] = mask[i] & notMask[i];
      }
      return;
    }

    // Field filter
    if ('field' in condition && 'filter' in condition) {
      const { field, filter } = condition;
      const fieldName = field as string;

      // Validate that the field exists before evaluating the filter
      this.validateField(fieldName);

      this.evaluateBasicFilter(table, fieldName, filter, mask);
      return;
    }
  }

  /**
   * Evaluate a basic field filter using vectorized operations
   */
  private evaluateBasicFilter(
    table: Table,
    field: string,
    filter: filters.BasicFilter<any>,
    mask: Uint8Array,
  ): void {
    const vector = table.getChild(field);

    // If vector doesn't exist after validation, something is wrong with the table structure
    if (!vector) {
      throw new Error(
        `Implementation error: Field '${field}' exists in schema but not in table data.`,
      );
    }

    const numRows = table.numRows;
    const batchSize = 1000; // Process in batches for better cache efficiency

    // Extract filter info
    const { op, value } = filter;

    // Optimization for equality checks
    if (op === 'eq') {
      for (let i = 0; i < numRows; i += batchSize) {
        const end = Math.min(i + batchSize, numRows);
        for (let j = i; j < end; j++) {
          if (mask[j] === 0) continue; // Already filtered out

          // Special handling for null value filter
          if (value === null) {
            // If we're looking for nulls, keep only rows where the value is null
            if (vector.isValid(j)) {
              mask[j] = 0; // Not null, so filter it out
            }
            continue;
          }

          // For non-null values, filter out nulls
          if (!vector.isValid(j)) {
            mask[j] = 0;
            continue;
          }

          // Direct comparison (faster than switch-case for hot path) for non-null values
          if (vector.get(j) !== value) {
            mask[j] = 0;
          }
        }
      }
      return; // Done with this filter
    }

    // Not equal operator
    if (op === 'neq') {
      for (let i = 0; i < numRows; i += batchSize) {
        const end = Math.min(i + batchSize, numRows);
        for (let j = i; j < end; j++) {
          if (mask[j] === 0) continue; // Already filtered out

          // Special handling for null value filter
          if (value === null) {
            // If we're looking for not-nulls, keep only rows where the value is not null
            if (!vector.isValid(j)) {
              mask[j] = 0; // Is null, so filter it out
            }
            continue;
          }

          // For non-null filter values
          if (!vector.isValid(j)) {
            // Keep nulls when comparing with non-null value (null != 'something' is true)
            continue;
          }

          // Direct comparison for non-null values
          if (vector.get(j) === value) {
            mask[j] = 0; // Equal, so filter it out for inequality check
          }
        }
      }
      return;
    }

    // Optimization for numeric range operations
    if (['gt', 'gte', 'lt', 'lte'].includes(op)) {
      for (let i = 0; i < numRows; i += batchSize) {
        const end = Math.min(i + batchSize, numRows);
        for (let j = i; j < end; j++) {
          if (mask[j] === 0) continue;

          // If filter value is null, no rows should match comparison operators
          if (value === null) {
            mask[j] = 0;
            continue;
          }

          // Filter out null values for comparison operations
          if (!vector.isValid(j)) {
            mask[j] = 0;
            continue;
          }

          const cellValue = vector.get(j);
          if (typeof cellValue !== 'number' && typeof value === 'number') {
            mask[j] = 0;
            continue;
          }

          // Unrolled comparison logic for performance
          let keep = false;
          if (op === 'gt') keep = cellValue > value;
          else if (op === 'gte') keep = cellValue >= value;
          else if (op === 'lt') keep = cellValue < value;
          else if (op === 'lte') keep = cellValue <= value;

          if (!keep) mask[j] = 0;
        }
      }
      return; // Done with this filter
    }

    // String operations
    if (['contains', 'startsWith', 'endsWith'].includes(op)) {
      for (let i = 0; i < numRows; i += batchSize) {
        const end = Math.min(i + batchSize, numRows);
        for (let j = i; j < end; j++) {
          if (mask[j] === 0) continue;

          // If filter value is null, no rows should match string operations
          if (value === null) {
            mask[j] = 0;
            continue;
          }

          // Filter out null values for string operations
          if (!vector.isValid(j)) {
            mask[j] = 0;
            continue;
          }

          const cellValue = vector.get(j);
          // String operation can only be performed on string values
          if (typeof cellValue !== 'string') {
            mask[j] = 0;
            continue;
          }

          let keep = false;
          if (op === 'contains') keep = cellValue.includes(String(value));
          else if (op === 'startsWith')
            keep = cellValue.startsWith(String(value));
          else if (op === 'endsWith') keep = cellValue.endsWith(String(value));

          if (!keep) mask[j] = 0;
        }
      }
      return;
    }

    // Array (in) operation
    if (op === 'in') {
      for (let i = 0; i < numRows; i += batchSize) {
        const end = Math.min(i + batchSize, numRows);
        for (let j = i; j < end; j++) {
          if (mask[j] === 0) continue;

          // Special handling if the filter array includes null
          if (Array.isArray(value) && value.includes(null)) {
            if (!vector.isValid(j)) {
              // Keep null values if null is in the filter array
              continue;
            }
          } else {
            // If null is not in the filter array, filter out null values
            if (!vector.isValid(j)) {
              mask[j] = 0;
              continue;
            }
          }

          const cellValue = vector.get(j);
          if (!Array.isArray(value) || !value.includes(cellValue)) {
            mask[j] = 0;
          }
        }
      }
      return;
    }

    // Fallback for any unhandled operator
    console.warn(`Unhandled filter operator: ${op}`);
    // Don't modify the mask for unhandled operators
  }

  /**
   * Apply a filter mask to create a new table with only the rows where mask[i] === 1
   * @param table The Arrow table
   * @param mask Uint8Array with 1s for rows to keep
   * @returns Filtered Arrow table
   */
  private applyFilterMask(table: Table, mask: Uint8Array): Table {
    // Count how many rows will be in the result
    const resultRowCount = mask.reduce((sum, val) => sum + val, 0);

    if (resultRowCount === 0) {
      return makeTable({});
    }

    if (resultRowCount === table.numRows) {
      return table;
    }

    // For both small and large result sets, use an optimized approach
    const filteredVectors: Record<string, any[]> = {};

    // Initialize arrays for each column with pre-allocated capacity
    table.schema.fields.forEach((field) => {
      // Pre-allocate arrays with exact size for better performance
      filteredVectors[field.name] = new Array(resultRowCount);
    });

    // Process in batches with a single pass through the mask
    let resultIndex = 0;
    const batchSize = 1000;

    for (let i = 0; i < table.numRows; i += batchSize) {
      const end = Math.min(i + batchSize, table.numRows);

      // Collect indices to keep in this batch
      const indices: number[] = [];
      for (let j = i; j < end; j++) {
        if (mask[j] === 1) {
          indices.push(j);
        }
      }

      // Skip batch if nothing to keep
      if (indices.length === 0) continue;

      // Extract data for each field
      table.schema.fields.forEach((field) => {
        const vector = table.getChild(field.name);
        if (vector) {
          for (let idx = 0; idx < indices.length; idx++) {
            const rowIdx = indices[idx];
            filteredVectors[field.name][resultIndex + idx] = vector.isValid(
              rowIdx,
            )
              ? vector.get(rowIdx)
              : null;
          }
        } else {
          // Fill with nulls if column doesn't exist
          for (let idx = 0; idx < indices.length; idx++) {
            filteredVectors[field.name][resultIndex + idx] = null;
          }
        }
      });

      resultIndex += indices.length;
    }

    // Create new table from filtered data
    return tableFromArrays(filteredVectors);
  }

  private validateFilterCondition(condition: filters.FilterCondition<T>): void {
    if ('AND' in condition) {
      // Validate each condition in AND array
      for (const subCondition of condition.AND) {
        this.validateFilterCondition(subCondition);
      }
      return;
    }

    if ('OR' in condition) {
      // Validate each condition in OR array
      for (const subCondition of condition.OR) {
        this.validateFilterCondition(subCondition);
      }
      return;
    }

    if ('NOT' in condition) {
      // Validate the NOT condition
      this.validateFilterCondition(condition.NOT);
      return;
    }

    if ('field' in condition && 'filter' in condition) {
      // Validate that the field exists
      this.validateField(condition.field as string);
      return;
    }
  }

  private validateFilterFields(filters: filters.FilterCondition<T>[]): void {
    for (const condition of filters) {
      this.validateFilterCondition(condition);
    }
  }

  /**
   * Apply filters with support for complex filter conditions
   */
  filter<R extends T = T>(filters: filters.Filters<T>): ArrowStore<R> {
    return this.createInstance<R>(async (table) => {
      try {
        // Before creating the filter mask, validate all field references in the filters
        this.validateFilterFields(filters);

        // Create a filter mask using vectorized operations
        const mask = this.createComplexFilterMask(table, filters);

        // Apply the filter mask
        return {
          table: this.applyFilterMask(table, mask),
          schema: this._schema,
        };
      } catch (error) {
        // Enhance error messages with more context
        if (error instanceof Error) {
          throw new Error(`Error filtering data: ${error.message}`);
        }
        throw error;
      }
    });
  }

  /**
   * Sort data based on options with vectorized approach where possible
   */
  sort(options: SortOptions): ArrowStore<T> {
    return this.createInstance<T>(
      async (table): Promise<{ table: Table<any>; schema?: TableSchema }> => {
        if (options.length === 0) {
          return { table };
        }

        // For simple sorts with a single column, we can optimize
        if (options.length === 1) {
          const { field, direction } = options[0];
          const vector = table.getChild(field);

          if (!vector) return { table };

          // Generate sort indices
          const indices = Array.from({ length: table.numRows }, (_, i) => i);

          // Sort indices based on vector values
          indices.sort((a, b) => {
            const aValue = vector.get(a);
            const bValue = vector.get(b);

            // Handle null values
            if (aValue === null || aValue === undefined) {
              return bValue === null || bValue === undefined ? 0 : -1;
            }
            if (bValue === null || bValue === undefined) {
              return 1;
            }

            // Compare values
            const comparison = compare(aValue, bValue);
            return direction === 'asc' ? comparison : -comparison;
          });

          // Create a new table with rows in sorted order
          const sortedColumns: Record<string, any[]> = {};

          // Initialize arrays for each column
          table.schema.fields.forEach((f) => {
            sortedColumns[f.name] = [];
          });

          // Fill arrays with values in sorted order
          indices.forEach((idx) => {
            table.schema.fields.forEach((f) => {
              const col = table.getChild(f.name);
              if (col) {
                sortedColumns[f.name].push(col.get(idx));
              } else {
                sortedColumns[f.name].push(null);
              }
            });
          });

          return {
            table: tableFromArrays(sortedColumns),
            schema: this._schema,
          };
        }

        // For complex multi-column sorts, use row-based approach
        // but process in batches for large tables
        return {
          table: await this.processBatches(
            table,
            10000, // Process 10K rows at a time
            (data) => {
              return [...data].sort((a, b) => {
                for (const option of options) {
                  const { field, direction } = option;
                  const aValue = a[field];
                  const bValue = b[field];

                  if (aValue === bValue) {
                    continue;
                  }

                  const comparison = compare(aValue, bValue);
                  return direction === 'asc' ? comparison : -comparison;
                }
                return 0;
              });
            },
          ),
          schema: this._schema,
        };
      },
    );
  }

  /**
   * Select specific fields - using Arrow's built-in select method
   */
  select<K extends keyof T>(fields: K[]): ArrowStore<Pick<T, K>> {
    // Create a new schema for the selected fields
    const _newSchema: TableSchema = {
      tableName: this._schema.tableName,
      columns: this._schema.columns.filter((col) =>
        fields.includes(col.name as K),
      ),
    };

    return this.createInstance<Pick<T, K>>(async (table) => {
      // Directly use Arrow's select method instead of our own implementation
      return { table: table.select(fields as string[]) };
    });
  }

  /**
   * Check if a map function is a simple column transformation
   * that can be vectorized
   */
  private isSimpleColumnTransform<R>(
    mapFn: (item: T) => R,
    sampleItem?: T,
  ): boolean {
    try {
      // This is a simplistic check
      // In a real implementation, you'd analyze the function AST
      // or use a more sophisticated heuristic
      const fnStr = mapFn.toString();

      // Simple column transformations typically have patterns like:
      // item => ({ ...item, newField: item.field * 2 })
      // item => ({ field1: item.field1, field2: item.field2 + 1 })

      // Check for common patterns
      const isSimple =
        // Arrow functions that return an object literal
        /^\s*\(?(\w+)\)?\s*=>\s*\(\s*\{/.test(fnStr) ||
        // Arrow functions that just return a property
        /^\s*\(?(\w+)\)?\s*=>\s*\w+\.\w+/.test(fnStr) ||
        // Simple property transformations
        /^\s*function\s*\(\w+\)\s*\{\s*return\s*\{/.test(fnStr);

      return isSimple;
    } catch (e) {
      // If anything goes wrong with the analysis, assume it's complex
      return false;
    }
  }

  /**
   * Vectorized map for simple column transformations,
   * falls back to row-based for complex transformations.
   * Automatically generates a new schema based on the transformed data.
   */
  map<R extends Record<string, any>>(
    mapFn: (item: T) => R,
    resultSchema?: TableSchema,
  ): ArrowStore<R> {
    return this.createInstance<R>(
      async (table): Promise<{ table: Table<any>; schema: TableSchema }> => {
        try {
          // Try to determine if this is a simple transform that can be vectorized
          if (table.numRows > 0 && this.isSimpleColumnTransform(mapFn)) {
            try {
              // Get a sample item to determine output structure
              const sampleInput = table.get(0) as T;
              const sampleOutput = mapFn(sampleInput);

              // Get output field names and infer types
              const outputFields = Object.keys(sampleOutput);

              // Prepare output columns
              const result: Record<string, any[]> = {};
              outputFields.forEach((field) => {
                result[field] = [];
              });

              // Process each row
              for (let i = 0; i < table.numRows; i++) {
                const row = table.get(i) as T;
                const transformed = mapFn(row);

                // Add each field to its column
                outputFields.forEach((field) => {
                  result[field].push(transformed[field]);
                });
              }

              // Create output table
              const outputTable = tableFromArrays(result);

              // Generate new schema based on the transformed data
              const newSchema = inferSchemaFromTransformedData(
                outputTable,
                sampleOutput,
                this.getSchema().tableName,
              );

              return { table: outputTable, schema: newSchema };
            } catch (e) {
              // If vectorized approach fails, fall back to row-based
              console.warn(
                'Vectorized map failed, falling back to row-based processing',
                e,
              );
            }
          }

          // For complex transformations or on failure, use batch processing
          const outputTable = await this.processBatches(
            table,
            10000, // Process 10K rows at a time
            (data) => data.map(mapFn as any) as R[],
          );

          // With batch processing, we need to infer schema from the result
          // Get a sample of the transformed data if available
          let sampleOutput: R | undefined;
          if (outputTable.numRows > 0) {
            sampleOutput = outputTable.get(0) as R;
          }

          // Generate new schema
          const newSchema = inferSchemaFromTransformedData(
            outputTable,
            sampleOutput,
            this.getSchema().tableName,
          );

          return { table: outputTable, schema: newSchema };
        } catch (error) {
          throw new Error(
            `Error in map operation: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }
      },
    );
  }

  /**
   * Reduce operation for ArrowStore
   * Applies a reducer function to each row and accumulates a result
   *
   * @param reducer Function that accumulates values
   * @param initialValue Initial accumulator value
   * @returns Promise with the accumulated result
   */
  async reduce<R>(
    reducer: (accumulator: R, current: T, index: number) => R,
    initialValue: R,
  ): Promise<R> {
    try {
      const resolvedTable = await this.resolveTable();

      if (resolvedTable.numRows === 0) {
        return initialValue;
      }

      // For large tables, process in batches to avoid memory issues
      let result = initialValue;
      const batchSize = 10000; // Process 10K rows at a time

      for (let i = 0; i < resolvedTable.numRows; i += batchSize) {
        const end = Math.min(i + batchSize, resolvedTable.numRows);

        // Get a slice of the table for this batch
        const batchTable = resolvedTable.slice(i, end);
        const batchData = batchTable.toArray();

        // Apply reducer to each row in the batch
        for (let j = 0; j < batchData.length; j++) {
          result = reducer(result, batchData[j] as T, i + j);
        }

        // Allow GC to reclaim memory between batches
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      return result;
    } catch (error) {
      throw new Error(
        `Error in reduce operation: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Fold operation for ArrowStore
   * Alias for reduce
   */
  async fold<R>(
    folder: (accumulator: R, current: T, index: number) => R,
    initialValue: R,
  ): Promise<R> {
    return this.reduce(folder, initialValue);
  }

  /**
   * Reduce Right operation for ArrowStore
   * Like reduce, but processes items from right to left (last to first)
   *
   * @param reducer Function that accumulates values
   * @param initialValue Initial accumulator value
   * @returns Promise with the accumulated result
   */
  async reduceRight<R>(
    reducer: (accumulator: R, current: T, index: number) => R,
    initialValue: R,
  ): Promise<R> {
    try {
      const resolvedTable = await this.resolveTable();

      if (resolvedTable.numRows === 0) {
        return initialValue;
      }

      // For large tables, process in batches to avoid memory issues
      let result = initialValue;
      const batchSize = 10000; // Process 10K rows at a time

      // Process from right to left (last row to first)
      for (let i = resolvedTable.numRows - 1; i >= 0; i -= batchSize) {
        const start = Math.max(0, i - batchSize + 1);

        // Get a slice of the table for this batch
        const batchTable = resolvedTable.slice(start, i + 1);
        const batchData = batchTable.toArray();

        // Apply reducer to each row in the batch from right to left
        for (let j = batchData.length - 1; j >= 0; j--) {
          const globalIndex = start + j;
          result = reducer(result, batchData[j] as T, globalIndex);
        }

        // Allow GC to reclaim memory between batches
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      return result;
    } catch (error) {
      throw new Error(
        `Error in reduceRight operation: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Fold Right operation for ArrowStore
   * Alias for reduceRight
   */
  async foldRight<R>(
    folder: (accumulator: R, current: T, index: number) => R,
    initialValue: R,
  ): Promise<R> {
    return this.reduceRight(folder, initialValue);
  }

  /**
   * Convert the table to a Set based on a key function
   *
   * @param keyFn Function to extract a unique key from each row
   * @returns Promise with a Set of unique values
   */
  async toSet<K>(keyFn?: (item: T) => K): Promise<Set<K | T>> {
    try {
      const resolvedTable = await this.resolveTable();
      const uniqueMap = new Map<string, K>();

      if (resolvedTable.numRows === 0) {
        return new Set();
      }

      // Process in batches for better memory management
      const batchSize = 10000;

      for (let i = 0; i < resolvedTable.numRows; i += batchSize) {
        const end = Math.min(i + batchSize, resolvedTable.numRows);

        // Get a slice of the table for this batch
        const batchTable = resolvedTable.slice(i, end);
        const batchData = batchTable.toArray();

        // Add items or keys to the set
        if (keyFn) {
          for (const item of batchData) {
            const val = keyFn(item as T);
            const key = JSON.stringify(val);

            // convert to string to handle complex keys
            uniqueMap.set(key, val);
          }
        } else {
          for (const item of batchData) {
            const compositeKey = JSON.stringify(item);
            uniqueMap.set(compositeKey, item as K);
          }
        }

        // Allow GC to reclaim memory between batches
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      return new Set(uniqueMap.values());
    } catch (error) {
      throw new Error(
        `Error in toSet operation: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Convert the table to a Map based on a key function
   *
   * @param keyFn Function to extract a key from each row
   * @param valueFn Optional function to transform each row into a value (defaults to identity)
   * @returns Promise with a Map of key-value pairs
   */
  async toMap<K, V = T>(
    keyFn: (item: T) => K,
    valueFn?: (item: T) => V,
  ): Promise<Map<K, V>> {
    try {
      const resolvedTable = await this.resolveTable();
      const resultMap = new Map<K, V>();

      if (resolvedTable.numRows === 0) {
        return resultMap;
      }

      // Process in batches for better memory management
      const batchSize = 10000;

      for (let i = 0; i < resolvedTable.numRows; i += batchSize) {
        const end = Math.min(i + batchSize, resolvedTable.numRows);

        // Get a slice of the table for this batch
        const batchTable = resolvedTable.slice(i, end);
        const batchData = batchTable.toArray();

        // Add key-value pairs to the map
        for (const item of batchData as T[]) {
          const key = keyFn(item);
          const value = valueFn ? valueFn(item) : (item as unknown as V);
          resultMap.set(key, value);
        }

        // Allow GC to reclaim memory between batches
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      return resultMap;
    } catch (error) {
      throw new Error(
        `Error in toMap operation: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Group by a field and aggregate using vectorized operations where possible
   */
  groupBy<K extends keyof T, R extends Record<string, any>>(
    field: K,
    aggregations: Record<string, (values: any[]) => any>,
  ): ArrowStore<R> {
    // Create a new schema for the aggregated result
    const newColumns: ColumnSchema[] = [
      this.getColumnSchema(field as string) || {
        name: field as string,
        type: 'unknown',
      },
    ];

    Object.keys(aggregations).forEach((newField) => {
      newColumns.push({
        name: newField,
        type: 'unknown',
        nullable: true,
      });
    });

    const _newSchema: TableSchema = {
      tableName: `${this._schema.tableName}_grouped`,
      columns: newColumns,
    };

    return this.createInstance<R>(async (table) => {
      // Get the column to group by
      const groupByVector = table.getChild(field as string);

      if (!groupByVector) {
        return { table: new Table(table.schema) }; // Empty table if column not found
      }

      // First, identify unique values in the group-by column (vectorized)
      const uniqueValues = new Set<any>();
      const valueToIndices = new Map<any, number[]>();

      // First pass: collect unique values and row indices
      for (let i = 0; i < groupByVector.length; i++) {
        const value = groupByVector.get(i);

        // Skip null values
        if (value === null || value === undefined) continue;

        uniqueValues.add(value);

        if (!valueToIndices.has(value)) {
          valueToIndices.set(value, []);
        }

        valueToIndices.get(value)!.push(i);
      }

      // Prepare result structure
      const result: Record<string, any[]> = {
        [field as string]: Array.from(uniqueValues),
      };

      // Initialize aggregation columns
      Object.keys(aggregations).forEach((aggField) => {
        result[aggField] = [];
      });

      // Perform aggregations for each unique value
      for (const value of uniqueValues) {
        const indices = valueToIndices.get(value)!;

        // For each aggregation
        Object.entries(aggregations).forEach(([aggField, aggFn]) => {
          // Get source field from the aggregation function if available
          const sourceField = (aggFn as any).sourceField;

          // Check if this is an optimizable aggregation from Aggregations helper
          if (
            aggFn.name === 'sum' ||
            aggFn.name === 'average' ||
            aggFn.name === 'min' ||
            aggFn.name === 'max' ||
            aggFn.name === 'count' ||
            aggFn.name === 'countDistinct'
          ) {
            // Skip optimization if sourceField is not provided
            if (!sourceField && aggFn.name !== 'count') {
              // For non-count operations, we need a source field
              result[aggField].push(aggFn(indices.map((i) => table.get(i))));
              return;
            }

            // For count operation, we don't need a source field
            if (aggFn.name === 'count') {
              result[aggField].push(indices.length);
              return;
            }

            // Get the column to aggregate
            const sourceVector = table.getChild(sourceField);

            if (sourceVector) {
              // Extract values for this group
              const values = indices
                .map((i) => sourceVector.get(i))
                .filter((v) => v !== null && v !== undefined);

              // Perform simple aggregation
              let aggregateResult: any;
              switch (aggFn.name) {
                case 'sum':
                  aggregateResult = values.reduce((sum, v) => sum + v, 0);
                  break;
                case 'average':
                  aggregateResult =
                    values.length > 0
                      ? values.reduce((sum, v) => sum + v, 0) / values.length
                      : null;
                  break;
                case 'min':
                  aggregateResult =
                    values.length > 0 ? Math.min(...values) : null;
                  break;
                case 'max':
                  aggregateResult =
                    values.length > 0 ? Math.max(...values) : null;
                  break;
                case 'countDistinct':
                  const distinctValues = new Set(values);
                  aggregateResult = distinctValues.size;
                  break;
              }

              result[aggField].push(aggregateResult);
              return;
            }
          }

          // For custom or complex aggregations
          // Get all rows for this group
          const rows = indices.map((i) => table.get(i));

          // Apply aggregation function
          result[aggField].push(aggFn(rows));
        });
      }

      return { table: tableFromArrays(result) };
    });
  }

  /**
   * Paginate data using Arrow's slice method
   */
  paginate(page: number, pageSize: number): ArrowStore<T> {
    return this.createInstance<T>(async (table) => {
      const start = (page - 1) * pageSize;
      const end = Math.min(start + pageSize, table.numRows);

      // Use Arrow's slice method which is more efficient
      return { table: table.slice(start, end) };
    });
  }

  /**
   * Slice operation for ArrowStore
   */
  slice(start: number, end: number): ArrowStore<T> {
    return this.createInstance<T>(async (table) => {
      // Use Arrow's slice method which is more efficient
      return { table: table.slice(start, end) };
    });
  }

  /**
   * Convert to RecordBatch - useful for interoperability
   */
  toRecordBatch(): Promise<RecordBatch> {
    return this.resolveTable().then((table) => {
      // If table has multiple batches, we consolidate them
      if (table.batches.length > 1) {
        // Create a temporary table from the data and get its batch
        const data = table.toArray();
        const tempTable = tableFromArrays(arrayToColumnarFormat(data));
        return tempTable.batches[0] || new RecordBatch(table.schema);
      }
      // Return the first batch if there's only one
      return table.batches[0] || new RecordBatch(table.schema);
    });
  }

  /**
   * Create a new ArrowStore with a subset of columns
   * Uses Arrow's native selectAt method
   */
  selectAt(columnIndices: number[]): ArrowStore<Record<string, any>> {
    const _newSchema = {
      tableName: this._schema.tableName,
      columns: columnIndices
        .map((i) => this._schema.columns[i])
        .filter(Boolean),
    };

    return this.createInstance<Record<string, any>>(async (table) => {
      return { table: table.selectAt(columnIndices) };
    });
  }

  /**
   * Combine with another ArrowStore
   * Uses Arrow's native assign method
   */
  combine<R extends Record<string, any>>(
    other: ArrowStore<R>,
  ): Promise<ArrowStore<T & R>> {
    return this.resolveTable().then(async (table) => {
      const otherTable = await other.resolveTable();

      // Use Arrow's native assign method to merge tables
      const combinedTable = table.assign(otherTable);

      // Create a new schema by combining both schemas
      const combinedSchema: TableSchema = {
        tableName: this._schema.tableName,
        columns: [
          ...this._schema.columns,
          ...other._schema.columns.filter(
            (col) => !this._schema.columns.some((c) => c.name === col.name),
          ),
        ],
      };

      return new ArrowStore<T & R>(combinedTable, combinedSchema);
    });
  }

  /**
   * Get a single column as a Vector
   */
  getColumn<K extends keyof T>(columnName: K): Vector<any> | null {
    return this.getColumnAt(
      this._schema.columns.findIndex((col) => col.name === columnName),
    );
  }

  /**
   * Get a single column as a Vector by index
   */
  getColumnAt(columnIndex: number): Vector<any> | null {
    if (columnIndex < 0 || columnIndex >= this._schema.columns.length) {
      return null;
    }

    // Use Arrow's getChildAt method
    return this.#table.getChildAt(columnIndex);
  }

  /**
   * Optimized vectorized sum operation for numeric columns
   */
  sumColumn(columnName: string): Promise<number> {
    return this.resolveTable().then((table) => {
      const vector = table.getChild(columnName);
      if (!vector) return 0;

      // Try to access underlying data structure
      try {
        // This is a more cautious approach to access internal data
        const vectorData = (vector as any).data;
        if (vectorData && Array.isArray(vectorData) && vectorData.length > 0) {
          const chunk = vectorData[0];
          if (chunk && chunk.values) {
            const values = chunk.values;

            // If we have direct access to a TypedArray, use optimized summation
            if (ArrayBuffer.isView(values)) {
              let sum = 0;
              // Process in batches for better cache locality
              const batchSize = 1000;

              // Get the number of elements correctly based on the type of array
              let numElements = 0;

              if ('length' in values) {
                // For TypedArrays like Float64Array, Uint8Array, etc.
                numElements = (values as any).length;
              } else if (
                'byteLength' in values &&
                'BYTES_PER_ELEMENT' in values.constructor
              ) {
                // Alternative approach: calculate length from byteLength
                numElements = Math.floor(
                  values.byteLength /
                    (values.constructor as any).BYTES_PER_ELEMENT,
                );
              } else {
                // If we can't determine the length, fallback to standard approach
                throw new Error('Cannot determine array length');
              }

              for (let i = 0; i < numElements; i += batchSize) {
                const end = Math.min(i + batchSize, numElements);
                for (let j = i; j < end; j++) {
                  // Still need to check validity
                  if (vector.isValid(j)) {
                    const val = (values as any)[j];
                    if (typeof val === 'number' && !isNaN(val)) {
                      sum += val;
                    }
                  }
                }
              }
              return sum;
            }
          }
        }
      } catch (e) {
        // Log the error rather than silently ignoring it
        console.warn(`Error in optimized sumColumn for '${columnName}':`, e);
        // Continue to fallback approach
      }

      // Standard approach with optimization for batch processing
      let sum = 0;
      const length = vector.length;
      const batchSize = 1000;

      for (let i = 0; i < length; i += batchSize) {
        const end = Math.min(i + batchSize, length);
        for (let j = i; j < end; j++) {
          if (vector.isValid(j)) {
            const value = vector.get(j);
            if (typeof value === 'number' && !isNaN(value)) {
              sum += value;
            }
          }
        }
      }

      return sum;
    });
  }

  /**
   * Vectorized average calculation for numeric columns
   */
  averageColumn(columnName: string): Promise<number | null> {
    return this.resolveTable().then((table) => {
      const vector = table.getChild(columnName);
      if (!vector) return null;

      let sum = 0;
      let count = 0;

      for (let i = 0; i < vector.length; i++) {
        if (vector.isValid(i)) {
          const value = vector.get(i);
          if (typeof value === 'number') {
            sum += value;
            count++;
          }
        }
      }

      return count > 0 ? sum / count : null;
    });
  }

  /**
   * Vectorized min/max calculation
   */
  minMaxColumn(columnName: string): Promise<{ min: any; max: any }> {
    return this.resolveTable().then((table) => {
      const vector = table.getChild(columnName);
      if (!vector || vector.length === 0) {
        return { min: null, max: null };
      }

      let min: any = null;
      let max: any = null;
      let hasValue = false;

      for (let i = 0; i < vector.length; i++) {
        if (vector.isValid(i)) {
          const value = vector.get(i);

          // Skip null/undefined values
          if (value === null || value === undefined) continue;

          // Initialize min/max if this is the first value
          if (!hasValue) {
            min = max = value;
            hasValue = true;
            continue;
          }

          // Update min/max
          if (compare(value, min) < 0) min = value;
          if (compare(value, max) > 0) max = value;
        }
      }

      return { min, max };
    });
  }

  /**
   * Vectorized distinct value count
   */
  countDistinct(columnName: string): Promise<number> {
    return this.resolveTable().then((table) => {
      const vector = table.getChild(columnName);
      if (!vector) return 0;

      const distinctValues = new Set();

      for (let i = 0; i < vector.length; i++) {
        if (vector.isValid(i)) {
          const value = vector.get(i);
          distinctValues.add(value);
        }
      }

      return distinctValues.size;
    });
  }

  /**
   * Perform a join operation with another table
   * @param other Other data store to join with
   * @param joinKey Key to join on
   */
  join<R extends Record<string, any>>(
    other: ArrowStore<R>,
    joinKey: keyof T & keyof R,
  ): Promise<ArrowStore<T & R>> {
    return this.resolveTable().then(async (leftTable) => {
      const rightTable = await other.resolveTable();

      // Get join columns
      const leftVector = leftTable.getChild(joinKey as string);
      const rightVector = rightTable.getChild(joinKey as string);

      if (!leftVector || !rightVector) {
        throw new Error(
          `Join key "${String(joinKey)}" not found in both tables`,
        );
      }

      // Create lookup map from right table
      const rightMap = new Map<any, number[]>();

      for (let i = 0; i < rightVector.length; i++) {
        if (rightVector.isValid(i)) {
          const key = rightVector.get(i);

          if (!rightMap.has(key)) {
            rightMap.set(key, []);
          }

          rightMap.get(key)!.push(i);
        }
      }

      // Create joined rows
      const joinedRows: (T & R)[] = [];

      for (let leftIdx = 0; leftIdx < leftTable.numRows; leftIdx++) {
        if (!leftVector.isValid(leftIdx)) continue;

        const key = leftVector.get(leftIdx);
        const leftRow = leftTable.get(leftIdx) as T;

        // Find matching rows in right table
        const rightIndices = rightMap.get(key) || [];

        if (rightIndices.length === 0) {
          // No match (inner join skips these)
          continue;
        }

        // Join with each matching row
        for (const rightIdx of rightIndices) {
          const rightRow = rightTable.get(rightIdx) as R;

          // Merge rows
          const joinedRow = {
            ...leftRow,
            ...rightRow,
          };

          joinedRows.push(joinedRow as T & R);
        }
      }

      // Create merged schema
      const mergedSchema: TableSchema = {
        tableName: `${this._schema.tableName}_joined`,
        columns: [
          ...this._schema.columns,
          ...other._schema.columns.filter(
            (col) =>
              col.name !== joinKey &&
              !this._schema.columns.some((c) => c.name === col.name),
          ),
        ],
      };

      // Create new table from joined rows
      const joinedTable = tableFromArrays(arrayToColumnarFormat(joinedRows));

      return new ArrowStore<T & R>(joinedTable, mergedSchema);
    });
  }

  /**
   * Filter using a JSON string representation of filters
   * @param jsonFilters JSON string containing serialized filters
   */
  filterFromJson<R extends T = T>(jsonFilters: string): ArrowStore<R> {
    try {
      // Parse the JSON string to get filter conditions
      const filters = filtersFromJson<T>(jsonFilters);

      // Use the standard filter method
      return this.filter<R>(filters) as ArrowStore<R>;
    } catch (error) {
      console.error('Error parsing filter JSON:', error);
      // Return unfiltered store if there's an error
      return this.createInstance<R>(async (table) => ({
        table,
      })) as ArrowStore<R>;
    }
  }

  /**
   * Filter using a serialized representation of filters (plain objects)
   * @param serializedFilters Array of serialized filter conditions
   */
  filterFromSerialized<R extends T = T>(
    serializedFilters: SerializedFilterCondition[],
  ): ArrowStore<R> {
    try {
      // Deserialize the filter conditions
      const filters = deserializeFilters<T>(serializedFilters);

      // Use the standard filter method
      return this.filter<R>(filters) as ArrowStore<R>;
    } catch (error) {
      console.error('Error deserializing filters:', error);
      // Return unfiltered store if there's an error
      return this.createInstance<R>(async (table) => ({
        table,
      })) as ArrowStore<R>;
    }
  }

  /**
   * Filter using a simple object with field-value pairs (implicit equality)
   * @param simpleFilters Object where keys are field names and values are what to equal-match
   */
  filterEquals<R extends T = T>(simpleFilters: Partial<T>): ArrowStore<R> {
    try {
      // Convert simple field-value pairs to equality filters
      const filters: filters.FilterCondition<T>[] = Object.entries(
        simpleFilters,
      ).map(([fieldName, value]) => ({
        field: fieldName as keyof T,
        filter: {
          op: 'eq' as filters.FilterOperator,
          value,
        },
      }));

      // Use the standard filter method
      return this.filter<R>(filters) as ArrowStore<R>;
    } catch (error) {
      console.error('Error converting simple filters:', error);
      // Return unfiltered store if there's an error
      return this.createInstance<R>(async (table) => ({
        table,
      })) as ArrowStore<R>;
    }
  }

  /**
   * Create and serialize current filter conditions
   * This is useful for saving the current filter state
   * @param filters Filter conditions to serialize
   * @returns JSON string of serialized filters
   */
  serializeFilters(filters: filters.FilterCondition<T>[]): string {
    return filtersToJson(filters);
  }

  /**
   * Create filter conditions from a URL query string
   * Useful for handling filter parameters in REST API endpoints
   * @param queryString URL query string (e.g., "status=active&minAge=25&department=engineering,product")
   */
  filterFromQueryString<R extends T = T>(queryString: string): ArrowStore<R> {
    try {
      // Parse the query string
      const params = new URLSearchParams(queryString);
      const filters: filters.FilterCondition<T>[] = [];

      // Convert query parameters to filters
      for (const [key, value] of params.entries()) {
        if (!value) continue;

        // Handle comma-separated values as "in" operator
        if (value.includes(',')) {
          filters.push({
            field: key as keyof T,
            filter: {
              op: 'in',
              value: value.split(','),
            },
          });
          continue;
        }

        // Handle numeric values
        if (!isNaN(Number(value))) {
          filters.push({
            field: key as keyof T,
            filter: {
              op: 'eq',
              value: Number(value),
            },
          });
          continue;
        }

        // Handle boolean values
        if (value === 'true' || value === 'false') {
          filters.push({
            field: key as keyof T,
            filter: {
              op: 'eq',
              value: value === 'true',
            },
          });
          continue;
        }

        // Handle range values (min/max prefix)
        if (key.startsWith('min')) {
          const fieldName =
            key.substring(3, 4).toLowerCase() + key.substring(4);
          filters.push({
            field: fieldName as keyof T,
            filter: {
              op: 'gte',
              value: Number(value),
            },
          });
          continue;
        }

        if (key.startsWith('max')) {
          const fieldName =
            key.substring(3, 4).toLowerCase() + key.substring(4);
          filters.push({
            field: fieldName as keyof T,
            filter: {
              op: 'lte',
              value: Number(value),
            },
          });
          continue;
        }

        // Default to string equality
        filters.push({
          field: key as keyof T,
          filter: {
            op: 'eq',
            value: value,
          },
        });
      }

      // Use the standard filter method
      return this.filter<R>(filters);
    } catch (error) {
      console.error('Error parsing query string filters:', error);
      // Return unfiltered store if there's an error
      return this.createInstance<R>(async (table) => ({ table }));
    }
  }

  /**
   * Convert the current filters to a URL query string
   * Useful for creating sharable URLs with the current filter state
   * @param filters Array of filter conditions
   * @returns URL query string
   */
  filtersToQueryString(filters: filters.FilterCondition<T>[]): string {
    const params = new URLSearchParams();

    for (const filter of filters) {
      if ('field' in filter && 'filter' in filter) {
        const { field, filter: condition } = filter;

        // Handle different operators
        switch (condition.op) {
          case 'eq':
            params.append(field as string, String(condition.value));
            break;

          case 'gt':
            params.append(
              `min${capitalizeFirstLetter(field as string)}`,
              String(condition.value),
            );
            break;

          case 'lt':
            params.append(
              `max${capitalizeFirstLetter(field as string)}`,
              String(condition.value),
            );
            break;

          case 'in':
            if (Array.isArray(condition.value)) {
              params.append(field as string, condition.value.join(','));
            }
            break;

          // Add other cases as needed
        }
      }
      // Complex conditions (AND, OR, NOT) aren't easily represented in URL params
      // For complex filters, consider using a different serialization method
    }

    return params.toString();
  }

  /**
   * Filter using a SQL-like WHERE clause string
   * @param sqlFilter SQL-like WHERE clause string (without the WHERE keyword)
   * @returns Filtered data store
   *
   * @example
   * // Simple equality
   * store.filterSql("department = 'Engineering'")
   *
   * @example
   * // Multiple conditions
   * store.filterSql("isActive = true AND (department = 'Engineering' OR department = 'Product')")
   *
   * @example
   * // Complex conditions
   * store.filterSql("age >= 25 AND age <= 40 AND department IN ('Engineering', 'Product') AND name LIKE '%Manager%'")
   */
  filterSql<R extends T = T>(sqlFilter: string): ArrowStore<R> {
    try {
      // Parse the SQL-like filter string
      const filters = parseSqlFilter<T>(sqlFilter);

      // Validate all fields before applying the filter
      this.validateFilterFields(filters);

      // Use the standard filter method
      return this.filter<R>(filters);
    } catch (error) {
      // console.error('Error parsing or validating SQL filter:', error);
      throw new Error(
        `Invalid SQL filter: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Filter using a full SQL query string
   * Supports basic SELECT statements with WHERE clauses
   *
   * @param sql SQL query string
   * @returns Filtered and projected data store
   *
   * @example
   * // Basic query
   * store.sql("SELECT * FROM users WHERE department = 'Engineering'")
   *
   * @example
   * // With projection
   * store.sql("SELECT name, age, department FROM users WHERE age > 30")
   */
  sql<R extends Record<string, any> = T>(sql: string): ArrowStore<R> {
    try {
      // Very basic SQL parsing - this could be expanded significantly
      sql = sql.trim();

      // Check if it's a SELECT statement
      if (!sql.toUpperCase().startsWith('SELECT ')) {
        throw new Error('Only SELECT statements are supported');
      }

      // Extract the parts of the query
      const fromIndex = sql.toUpperCase().indexOf(' FROM ');
      if (fromIndex === -1) {
        throw new Error('FROM clause is required');
      }

      // Extract projection (field list)
      const selectPart = sql.substring(7, fromIndex).trim();

      // Find the WHERE clause if it exists
      const whereIndex = sql.toUpperCase().indexOf(' WHERE ');
      const wherePart =
        whereIndex !== -1 ? sql.substring(whereIndex + 7).trim() : null;

      // Start with the base store
      let resultStore: ArrowStore<any> = this;

      // Apply WHERE clause if it exists
      if (wherePart) {
        resultStore = resultStore.filterSql(wherePart);
      }

      // Handle projections
      if (selectPart !== '*') {
        // Split the field list and trim whitespace
        const fields = selectPart.split(',').map((f) => f.trim());

        // Apply the projection
        resultStore = resultStore.select(fields);
      }

      return resultStore as ArrowStore<R>;
    } catch (error) {
      throw error;
    }
  }

  /**
   * Pivot operation for ArrowStore
   * Transforms rows into columns based on the provided options
   *
   * @param options Configuration for the pivot operation
   * @returns A new ArrowStore with the pivoted data
   */
  pivot<R extends Record<string, any> = Record<string, any>>(
    options: PivotOptions<T>,
  ): ArrowStore<R> {
    return this.createInstance<R>(async (table) => {
      const pivotTable = await pivot(table, options);
      return { table: pivotTable };
    });
  }

  /**
   * Performs an unpivot operation on an Arrow Table
   * Transforms columns into rows based on the provided options
   *
   * @param options Configuration for the unpivot operation
   * @returns A new ArrowStore with the unpivoted data
   */
  unpivot<R extends Record<string, any> = Record<string, any>>(
    options: UnpivotOptions<T>,
  ): ArrowStore<R> {
    const schema = this.getSchema();
    return this.createInstance<R>(async (table) => {
      const { table: unpivotTable, schema: nextSchema } = await unpivot(
        table,
        schema,
        options,
      );

      // Update the schema with the new columns
      this._schema = nextSchema;

      return { table: unpivotTable, schema: nextSchema };
    });
  }

  /**
   * Calculate ideal column widths for table rendering
   * @param options Configuration options for width calculation
   * @returns Map of column names to calculated widths in pixels or characters
   */
  async calculateColumnWidths(options: {
    // Whether to measure in pixels (true) or characters (false)
    pixelMeasurement?: boolean;
    // Custom font for pixel measurements
    font?: string;
    // Sample size for large tables (default: 100)
    sampleSize?: number;
    // Padding to add to each column (in pixels or characters)
    padding?: number;
    // Maximum width for any column (in pixels or characters)
    maxWidth?: number;
    // Minimum width for any column (in pixels or characters)
    minWidth?: number;
    // Custom width rules for specific columns
    columnOverrides?: Record<
      string,
      {
        minWidth?: number;
        maxWidth?: number;
        fixedWidth?: number;
      }
    >;
    // Character width estimates for pixel calculations (for different character types)
    charWidthEstimates?: {
      default: number;
      narrow: number; // i, l, etc.
      wide: number; // m, w, etc.
      numeric: number;
    };
  }): Promise<Map<string, number>> {
    const resolvedTable = await this.resolveTable();

    // Default options
    const pixelMeasurement = options.pixelMeasurement ?? false;
    const sampleSize = options.sampleSize ?? 100;
    const padding = options.padding ?? (pixelMeasurement ? 16 : 2); // 16px or 2 chars padding by default
    const maxWidth = options.maxWidth ?? (pixelMeasurement ? 300 : 50); // 300px or 50 chars max by default
    const minWidth = options.minWidth ?? (pixelMeasurement ? 60 : 10); // 60px or 10 chars min by default
    const charWidthEstimates = options.charWidthEstimates ?? {
      default: 8, // Default character width in pixels
      narrow: 4, // Width for narrow characters like 'i', 'l', etc.
      wide: 12, // Width for wide characters like 'm', 'w', etc.
      numeric: 7, // Width for numeric characters
    };

    // Initialize result map
    const columnWidths = new Map<string, number>();

    // Determine which rows to sample for large tables
    const numRows = resolvedTable.numRows;
    const rowsToSample = numRows <= sampleSize ? numRows : sampleSize;
    const sampleIndices = Array.from({ length: rowsToSample }, (_, i) =>
      Math.floor(i * (numRows / rowsToSample)),
    );

    // Process each column
    for (const field of resolvedTable.schema.fields) {
      const columnName = field.name;
      const vector = resolvedTable.getChild(columnName);
      if (!vector) continue;

      // Get column-specific overrides if any
      const columnOverride = options.columnOverrides?.[columnName];
      if (columnOverride?.fixedWidth) {
        columnWidths.set(columnName, columnOverride.fixedWidth);
        continue; // Skip width calculation for fixed-width columns
      }

      // Start with the header width
      let maxContentWidth = this.measureText(
        columnName,
        pixelMeasurement,
        charWidthEstimates,
      );

      // Sample values to determine max width
      for (const rowIndex of sampleIndices) {
        if (vector.isValid(rowIndex)) {
          const value = vector.get(rowIndex);
          if (value !== null && value !== undefined) {
            // Format the value based on its type
            const formattedValue = this.formatValueForDisplay(
              value,
              field.type,
            );
            const valueWidth = this.measureText(
              formattedValue,
              pixelMeasurement,
              charWidthEstimates,
            );
            maxContentWidth = Math.max(maxContentWidth, valueWidth);
          }
        }
      }

      // Apply padding
      let finalWidth = maxContentWidth + padding;

      // Apply column-specific constraints if provided
      const columnMinWidth = columnOverride?.minWidth ?? minWidth;
      const columnMaxWidth = columnOverride?.maxWidth ?? maxWidth;

      // Apply min/max constraints
      finalWidth = Math.max(
        columnMinWidth,
        Math.min(finalWidth, columnMaxWidth),
      );

      // Store the result
      columnWidths.set(columnName, finalWidth);
    }

    return columnWidths;
  }

  /**
   * Format a value for display based on its type
   * @param value The value to format
   * @param type The Arrow data type
   * @returns Formatted string representation
   */
  private formatValueForDisplay(value: any, type: any): string {
    if (value === null || value === undefined) {
      return '';
    }

    const typeStr = type.toString();

    if (value instanceof Date) {
      return value.toISOString();
    }

    if (Array.isArray(value)) {
      // Show just the array length for preview
      return `[${value.length}]`;
    }

    if (typeof value === 'object') {
      // For objects, show a truncated representation
      return JSON.stringify(value).substring(0, 50);
    }

    if (
      typeStr.includes('Decimal') ||
      typeStr.includes('Float') ||
      typeStr.includes('Double')
    ) {
      // Format numeric values with appropriate precision
      return typeof value === 'number' ? value.toFixed(2) : String(value);
    }

    // Default string conversion
    return String(value);
  }

  /**
   * Measure text width based on content
   * @param text The text to measure
   * @param pixelMeasurement Whether to measure in pixels (true) or characters (false)
   * @param charWidthEstimates Character width estimates for pixel calculations
   * @returns Estimated width in pixels or character count
   */
  private measureText(
    text: string,
    pixelMeasurement: boolean,
    charWidthEstimates: {
      default: number;
      narrow: number;
      wide: number;
      numeric: number;
    },
  ): number {
    if (!pixelMeasurement) {
      // Return length in characters
      return text.length;
    }

    // Perform pixel-based calculation
    let totalWidth = 0;

    for (let i = 0; i < text.length; i++) {
      const char = text[i];

      // Check character type
      if (/[il|.,;:]/.test(char)) {
        totalWidth += charWidthEstimates.narrow;
      } else if (/[mwWM]/.test(char)) {
        totalWidth += charWidthEstimates.wide;
      } else if (/[0-9]/.test(char)) {
        totalWidth += charWidthEstimates.numeric;
      } else {
        totalWidth += charWidthEstimates.default;
      }
    }

    return totalWidth;
  }

  /**
   * Get column statistics useful for width calculations
   * @param columnName The column to analyze
   * @param options Optional sampling and analysis options
   * @returns Statistical information about column values
   */
  async getColumnWidthStatistics(
    columnName: string,
    options?: {
      sampleSize?: number;
      includeFrequencies?: boolean;
    },
  ): Promise<{
    minLength: number;
    maxLength: number;
    avgLength: number;
    medianLength: number;
    mostFrequentLengths?: { length: number; count: number }[];
    uniqueValues?: number;
    nullCount: number;
  }> {
    const resolvedTable = await this.resolveTable();
    const vector = resolvedTable.getChild(columnName);

    if (!vector) {
      throw new Error(`Column "${columnName}" not found`);
    }

    // Default options
    const sampleSize = options?.sampleSize ?? Math.min(1000, vector.length);
    const includeFrequencies = options?.includeFrequencies ?? false;

    // Initialize statistics
    let minLength = Number.POSITIVE_INFINITY;
    let maxLength = 0;
    let totalLength = 0;
    let validCount = 0;
    let nullCount = 0;

    // Track value lengths and their frequencies
    const lengthFrequencies = new Map<number, number>();
    const stringLengths: number[] = [];
    const uniqueValues = new Set();

    // Determine sample indices
    const sampleIndices = this.generateSampleIndices(vector.length, sampleSize);

    // Process sampled values
    for (const i of sampleIndices) {
      if (vector.isValid(i)) {
        const value = vector.get(i);

        if (value === null || value === undefined) {
          nullCount++;
          continue;
        }

        validCount++;
        uniqueValues.add(value);

        // Format and measure the value
        const formatted = this.formatValueForDisplay(value, vector.type);
        const length = formatted.length;

        // Update statistics
        minLength = Math.min(minLength, length);
        maxLength = Math.max(maxLength, length);
        totalLength += length;
        stringLengths.push(length);

        // Update frequency map
        if (includeFrequencies) {
          lengthFrequencies.set(
            length,
            (lengthFrequencies.get(length) || 0) + 1,
          );
        }
      } else {
        nullCount++;
      }
    }

    // Calculate average
    const avgLength = validCount > 0 ? totalLength / validCount : 0;

    // Calculate median
    let medianLength = 0;
    if (stringLengths.length > 0) {
      stringLengths.sort((a, b) => a - b);
      const mid = Math.floor(stringLengths.length / 2);
      medianLength =
        stringLengths.length % 2 === 0
          ? (stringLengths[mid - 1] + stringLengths[mid]) / 2
          : stringLengths[mid];
    }

    // Prepare result
    const result: any = {
      minLength: minLength === Number.POSITIVE_INFINITY ? 0 : minLength,
      maxLength,
      avgLength,
      medianLength,
      nullCount,
      uniqueValues: uniqueValues.size,
    };

    // Add frequencies if requested
    if (includeFrequencies) {
      // Sort by count (descending)
      const mostFrequentLengths = Array.from(lengthFrequencies.entries())
        .map(([length, count]) => ({ length, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 10); // Top 10 most frequent lengths

      result.mostFrequentLengths = mostFrequentLengths;
    }

    return result;
  }

  /**
   * Generate a set of indices for sampling
   * @param totalCount Total number of items
   * @param sampleSize Desired sample size
   * @returns Array of indices to sample
   */
  private generateSampleIndices(
    totalCount: number,
    sampleSize: number,
  ): number[] {
    if (totalCount <= sampleSize) {
      // If we have fewer items than the sample size, return all indices
      return Array.from({ length: totalCount }, (_, i) => i);
    }

    if (sampleSize <= 100) {
      // For small samples, use evenly distributed indices
      return Array.from({ length: sampleSize }, (_, i) =>
        Math.floor(i * (totalCount / sampleSize)),
      );
    }

    // For larger samples, use a combination of approaches:
    // 1. Include the first and last few rows
    // 2. Sample evenly from the middle
    // 3. Add some random samples for diversity

    const result = new Set<number>();

    // Add first and last rows
    const edgeCount = Math.min(50, Math.floor(sampleSize * 0.1));
    for (let i = 0; i < edgeCount; i++) {
      result.add(i); // First rows
      result.add(totalCount - 1 - i); // Last rows
    }

    // Add evenly distributed samples
    const evenSampleCount = Math.floor(sampleSize * 0.6);
    for (let i = 0; i < evenSampleCount; i++) {
      result.add(
        Math.floor(
          edgeCount + i * ((totalCount - 2 * edgeCount) / evenSampleCount),
        ),
      );
    }

    // Add random samples to reach desired sample size
    while (result.size < sampleSize) {
      result.add(Math.floor(Math.random() * totalCount));
    }

    return Array.from(result);
  }

  /**
   * Calculate optimal column widths with auto-detection of data characteristics
   * @returns Map of column names to recommended widths
   */
  async getOptimalColumnWidths(): Promise<Map<string, number>> {
    const resolvedTable = await this.resolveTable();
    const result = new Map<string, number>();

    // Analyze each column
    for (const field of resolvedTable.schema.fields) {
      const columnName = field.name;

      try {
        // Get column statistics
        const stats = await this.getColumnWidthStatistics(columnName);

        // Calculate optimal width based on statistics and column type
        let optimalWidth: number;

        // Get data type to inform width decision
        const typeStr = field.type.toString();
        const isNumeric =
          typeStr.includes('Int') ||
          typeStr.includes('Float') ||
          typeStr.includes('Double') ||
          typeStr.includes('Decimal');
        const isBoolean = typeStr.includes('Bool');
        const isDate =
          typeStr.includes('Date') || typeStr.includes('Timestamp');

        if (isBoolean) {
          // Boolean columns are usually narrow
          optimalWidth = Math.max(columnName.length, 5) + 2; // "true" or "false" + padding
        } else if (isNumeric) {
          // Numeric columns - base on max length with some reasonable constraints
          optimalWidth = Math.min(
            Math.max(columnName.length, stats.maxLength) + 2,
            20,
          );
        } else if (isDate) {
          // Date columns have predictable widths
          optimalWidth = Math.max(columnName.length, 20) + 2; // ISO date format
        } else {
          // String and other types
          // Use a weighted approach based on average and maximum lengths
          const weightedLength = Math.ceil(
            (stats.avgLength * 2 + stats.medianLength * 2 + stats.maxLength) /
              5,
          );

          // Apply reasonable constraints
          optimalWidth = Math.min(
            Math.max(columnName.length, weightedLength) + 2,
            Math.max(50, stats.medianLength * 1.5),
          );
        }

        result.set(columnName, optimalWidth);
      } catch (error) {
        // If analysis fails, use a reasonable default
        result.set(columnName, Math.max(columnName.length, 15) + 2);
      }
    }

    return result;
  }

  /**
   * Calculate widths for fixed-width table output (for console or monospace display)
   * @param maxTableWidth Maximum width of the entire table (in characters)
   * @returns Map of column names to character widths
   */
  async getMonospaceTableColumnWidths(
    maxTableWidth?: number,
  ): Promise<Map<string, number>> {
    const resolvedTable = await this.resolveTable();
    const columnCount = resolvedTable.schema.fields.length;

    // Get basic width statistics for all columns
    const columnStats = new Map<
      string,
      Awaited<ReturnType<typeof this.getColumnWidthStatistics>>
    >();
    for (const field of resolvedTable.schema.fields) {
      columnStats.set(
        field.name,
        await this.getColumnWidthStatistics(field.name),
      );
    }

    // Calculate initial widths based on content
    const initialWidths = new Map<string, number>();
    let totalWidth = 0;

    for (const field of resolvedTable.schema.fields) {
      const stats = columnStats.get(field.name)!;

      // Use median as a starting point with some headroom
      const headerWidth = field.name.length;
      const contentWidth = Math.min(stats.medianLength * 1.2, stats.maxLength);
      const columnWidth = Math.max(headerWidth, contentWidth) + 2; // Add padding

      initialWidths.set(field.name, columnWidth);
      totalWidth += columnWidth;
    }

    // If max table width specified, adjust columns proportionally
    if (maxTableWidth && totalWidth > maxTableWidth) {
      // Calculate reduction factor
      const reductionFactor = maxTableWidth / totalWidth;

      // Apply reduction to all columns, respecting minimum widths
      let allocatedWidth = 0;
      const adjustedWidths = new Map<string, number>();

      // First pass - calculate proportional reductions
      for (const field of resolvedTable.schema.fields) {
        const initialWidth = initialWidths.get(field.name)!;
        const minWidth = Math.min(10, initialWidth); // Enforce reasonable minimum

        // Calculate new width but ensure it's not below minimum
        const newWidth = Math.max(
          minWidth,
          Math.floor(initialWidth * reductionFactor),
        );
        adjustedWidths.set(field.name, newWidth);
        allocatedWidth += newWidth;
      }

      // Second pass - if we have remaining space, distribute it to larger columns
      if (allocatedWidth < maxTableWidth) {
        const remainingWidth = maxTableWidth - allocatedWidth;

        // Sort columns by their original size (largest first)
        const sortedColumns = Array.from(initialWidths.entries())
          .sort((a, b) => b[1] - a[1])
          .map(([name]) => name);

        // Distribute remaining width
        let remaining = remainingWidth;
        let index = 0;

        while (remaining > 0 && index < sortedColumns.length) {
          const columnName = sortedColumns[index];
          adjustedWidths.set(columnName, adjustedWidths.get(columnName)! + 1);
          remaining--;
          index = (index + 1) % sortedColumns.length;
        }
      }

      return adjustedWidths;
    }

    return initialWidths;
  }
}
