import { type Table, tableFromArrays } from 'apache-arrow';
import {
  arrayToColumnarFormat,
  compare,
  generateCartesianProduct,
} from './helpers';
import type { TableSchema } from './types';

/**
 * Options for pivot operation
 */
export type PivotOptions<T extends Record<string, any>> = {
  /**
   * The column(s) to pivot on - these will become the new columns
   */
  on: keyof T | Array<keyof T>;

  /**
   * The value column(s) to use for the pivot cells
   * Each value can be a field name or an aggregation function
   */
  using: Array<
    | keyof T
    | {
        field: keyof T;
        aggregation: (values: any[]) => any;
        name?: string;
      }
  >;

  /**
   * The column(s) to group by - these will remain as rows
   */
  groupBy: Array<keyof T>;

  /**
   * Optional column ordering
   */
  orderBy?: Array<{
    field: string;
    direction: 'asc' | 'desc';
  }>;

  /**
   * Optional row limit
   */
  limit?: number;
};

/**
 * Options for unpivot operation
 */
export type UnpivotOptions<T extends Record<string, any>> = {
  /**
   * The column(s) to preserve as identifier columns
   */
  identifierColumns: Array<keyof T>;

  /**
   * The column(s) to unpivot into rows
   */
  valueColumns: Array<keyof T>;

  /**
   * Name for the new column that will contain the former column names
   */
  nameColumn: string;

  /**
   * Name for the new column that will contain the values
   */
  valueColumn: string;
};

/**
 * Performs a pivot operation on an Arrow Table
 * Transforms rows into columns based on the provided options
 *
 * @param table The Arrow Table to pivot
 * @param schema The schema of the table
 * @param options Configuration for the pivot operation
 * @returns A new Arrow Table with the pivoted data
 */
export async function pivot<T extends Record<string, any>>(
  table: Table,
  options: PivotOptions<T>,
): Promise<Table> {
  // Convert single 'on' column to array for consistent processing
  const onColumns = Array.isArray(options.on) ? options.on : [options.on];

  // Extract value column configurations
  const valueColumns = options.using.map((using) => {
    if (typeof using === 'object') {
      return {
        field: using.field,
        aggregation: using.aggregation,
        name: using.name || `${String(using.field)}`,
      };
    } else {
      return {
        field: using,
        aggregation: (values: any[]) => (values.length > 0 ? values[0] : null),
        name: String(using),
      };
    }
  });

  try {
    // 1. Get all distinct values for the pivot columns
    const distinctPivotValues: Record<string, Set<any>> = {};

    for (const col of onColumns) {
      const colVector = table.getChild(col as string);
      if (!colVector) continue;

      const valueSet = new Set<any>();
      for (let i = 0; i < colVector.length; i++) {
        if (colVector.isValid(i)) {
          valueSet.add(colVector.get(i));
        }
      }
      distinctPivotValues[col as string] = valueSet;
    }

    // Generate all combinations of pivot values if multiple pivot columns
    let pivotCombinations: Array<Record<string, any>>;

    if (onColumns.length === 1) {
      // Simple case: single pivot column
      const col = onColumns[0] as string;
      pivotCombinations = Array.from(distinctPivotValues[col]).map((val) => ({
        [col]: val,
        _pivotKey: String(val),
      }));
    } else {
      // Complex case: multiple pivot columns need cartesian product
      pivotCombinations = generateCartesianProduct(
        onColumns.map((col) =>
          Array.from(distinctPivotValues[col as string]).map((val) => ({
            [col as string]: val,
          })),
        ),
      );

      // Add a compound key for each combination
      pivotCombinations = pivotCombinations.map((combo) => ({
        ...combo,
        _pivotKey: onColumns
          .map((col) => String(combo[col as string]))
          .join('_'),
      }));
    }

    // Sort pivot combinations if orderBy is specified
    if (options.orderBy && options.orderBy.length > 0) {
      pivotCombinations.sort((a, b) => {
        for (const order of options.orderBy!) {
          // Extract field name without any aggregation prefix
          const fieldParts = order.field.split('.');
          const fieldName = fieldParts[fieldParts.length - 1];

          if (fieldName in a && fieldName in b) {
            const aVal = a[fieldName];
            const bVal = b[fieldName];

            if (aVal !== bVal) {
              return order.direction === 'asc'
                ? compare(aVal, bVal)
                : compare(bVal, aVal);
            }
          }
        }
        return 0;
      });
    }

    // 2. Group the data by the groupBy columns
    const groupedData = new Map<string, T[]>();

    // Extract the rows to process
    const rows = table.toArray() as T[];

    // Generate a composite key for each row based on groupBy columns
    for (const row of rows) {
      const groupKey = options.groupBy
        .map((col) => String(row[col as string]))
        .join('|');

      if (!groupedData.has(groupKey)) {
        groupedData.set(groupKey, []);
      }

      groupedData.get(groupKey)!.push(row);
    }

    // 3. Prepare the result data structure
    const result: Record<string, any>[] = [];

    // Process each group
    for (const [groupKey, groupRows] of groupedData.entries()) {
      // Create base result row with groupBy columns
      const resultRow: Record<string, any> = {};

      // Add groupBy columns to the result row
      const groupKeyParts = groupKey.split('|');
      for (let i = 0; i < options.groupBy.length; i++) {
        const col = options.groupBy[i];
        resultRow[col as string] = groupRows[0][col as string];
      }

      // Process each pivot combination for this group
      for (const pivotCombo of pivotCombinations) {
        // Filter the group rows that match this pivot combination
        const matchingRows = groupRows.filter((row) => {
          return onColumns.every(
            (col) => row[col as string] === pivotCombo[col as string],
          );
        });

        // For each value column, calculate the aggregated value
        for (const valueCol of valueColumns) {
          const columnValues = matchingRows.map(
            (row) => row[valueCol.field as string],
          );
          const aggregatedValue = valueCol.aggregation(columnValues);

          // 1. If a name was explicitly specified (different from the field name),
          //    then use the specified name prefixed to the pivot value
          // 2. If no name was specified or it's the same as the field,
          //    then just use the pivot value
          const wasNameExplicitlyProvided =
            valueCol.name !== String(valueCol.field);

          const columnName = wasNameExplicitlyProvided
            ? `${valueCol.name}_${pivotCombo._pivotKey}`
            : `${pivotCombo._pivotKey}`;

          resultRow[columnName] = aggregatedValue;
        }
      }

      result.push(resultRow);
    }

    // Apply limit if specified
    const limitedResult = options.limit
      ? result.slice(0, options.limit)
      : result;

    // Convert result to Arrow table
    return tableFromArrays(arrayToColumnarFormat(limitedResult));
  } catch (error) {
    throw new Error(
      `Error in pivot operation: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/**
 * Performs an unpivot operation on an Arrow Table
 * Transforms columns into rows based on the provided options
 *
 * @param table The Arrow Table to unpivot
 * @param schema The schema of the table
 * @param options Configuration for the unpivot operation
 * @returns A new Arrow Table with the unpivoted data
 */
export async function unpivot<T extends Record<string, any>>(
  table: Table,
  schema: TableSchema,
  options: UnpivotOptions<T>,
): Promise<{ table: Table; schema: TableSchema }> {
  try {
    // Create a new schema for the unpivoted data
    const newSchema: TableSchema = {
      tableName: schema.tableName,
      columns: [
        // Include identifier columns
        ...options.identifierColumns.map((colName) => {
          const originalCol = schema.columns.find(
            (col) => col.name === colName,
          );
          return originalCol || { name: colName as string, type: 'string' };
        }),
        // Add name column
        {
          name: options.nameColumn,
          type: 'string',
        },
        // Add value column
        {
          name: options.valueColumn,
          type: determineValueColumnType(
            schema,
            options.valueColumns as string[],
          ),
        },
      ],
    };

    // Convert Arrow table to rows for processing
    const rows = table.toArray() as T[];

    // Prepare the result data
    const result: Record<string, any>[] = [];

    // Process each input row
    for (const row of rows) {
      // For each value column to unpivot
      for (const valueCol of options.valueColumns) {
        // Create a new row for each value column
        const newRow: Record<string, any> = {};

        // Copy identifier columns
        for (const idCol of options.identifierColumns) {
          newRow[idCol as string] = row[idCol as string];
        }

        // Add the name and value columns
        newRow[options.nameColumn] = valueCol;
        newRow[options.valueColumn] = row[valueCol as string];

        result.push(newRow);
      }
    }

    // Convert to Arrow table
    return {
      table: tableFromArrays(arrayToColumnarFormat(result)),
      schema: newSchema,
    };
  } catch (error) {
    throw new Error(
      `Error in unpivot operation: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/**
 * Helper method to determine the type of the value column
 * based on the types of the original columns being unpivoted
 */
function determineValueColumnType(
  schema: TableSchema,
  valueColumns: Array<string>,
): string {
  // Get types of all value columns
  const types = valueColumns
    .map((colName) => {
      const col = schema.columns.find((c) => c.name === colName);
      return col ? col.type : 'unknown';
    })
    .filter((type) => type !== 'unknown');

  // If no valid types found, default to 'float'
  if (types.length === 0) return 'float';

  // If all columns have the same type, use that
  if (types.every((t) => t === types[0])) return types[0];

  // If mixed numeric types, use 'float'
  if (types.every((t) => t === 'integer' || t === 'float' || t === 'number'))
    return 'float';

  // Default to most flexible type
  return 'string';
}
