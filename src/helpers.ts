import type { Table } from 'apache-arrow';
import type { TableSchema } from './types';

// Helper function to capitalize the first letter of a string
export function capitalizeFirstLetter(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Helper function to map Apache Arrow type to string representation
 */
export function mapArrowTypeToString(arrowType: any): string {
  const typeStr = arrowType.toString();

  if (typeStr.includes('Int')) {
    return 'integer';
  } else if (typeStr.includes('Float') || typeStr.includes('Double')) {
    return 'float';
  } else if (typeStr.includes('Bool')) {
    return 'boolean';
  } else if (typeStr.includes('Date')) {
    return 'date';
  } else if (typeStr.includes('Timestamp')) {
    return 'timestamp';
  } else if (typeStr.includes('Time')) {
    return 'time';
  } else if (typeStr.includes('Utf8')) {
    return 'string';
  } else if (typeStr.includes('Binary')) {
    return 'binary';
  } else if (typeStr.includes('List')) {
    return 'array';
  } else if (typeStr.includes('Struct')) {
    return 'object';
  } else {
    return 'unknown';
  }
}

/**
 * Convert row-oriented array to column-oriented format for tableFromArrays
 */
export function arrayToColumnarFormat(
  rows: Record<string, any>[],
): Record<string, any[]> {
  if (rows.length === 0) {
    return {};
  }

  const result: Record<string, any[]> = {};
  const firstRow = rows[0];

  // Initialize arrays for each column
  Object.keys(firstRow).forEach((key) => {
    result[key] = [];
  });

  // Populate column arrays
  rows.forEach((row) => {
    Object.entries(row).forEach(([key, value]) => {
      result[key].push(value);
    });
  });

  return result;
}

/**
 * Helper function to compare values for sorting
 */
export function compare(a: any, b: any): number {
  if (a === null || a === undefined) {
    return b === null || b === undefined ? 0 : -1;
  }
  if (b === null || b === undefined) {
    return 1;
  }
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Helper function to generate cartesian product of value arrays
 */
export function generateCartesianProduct(
  arrays: Array<Record<string, any>[]>,
  current: Record<string, any> = {},
  index = 0,
  result: Array<Record<string, any>> = [],
): Array<Record<string, any>> {
  if (index === arrays.length) {
    result.push({ ...current });
    return result;
  }

  for (const item of arrays[index]) {
    generateCartesianProduct(
      arrays,
      { ...current, ...item },
      index + 1,
      result,
    );
  }

  return result;
}
/**
 * Helper method to infer a schema from transformed data
 * @param outputTable The transformed table
 * @param sampleOutput A sample of the transformed data (if available)
 * @param providedSchema Optional schema provided by the user
 * @returns A new TableSchema for the transformed data
 */
export function inferSchemaFromTransformedData<R extends Record<string, any>>(
  outputTable: Table,
  sampleOutput?: R,
  tableName?: string,
): TableSchema {
  // Start with the base table name from original schema
  const newSchema: TableSchema = {
    tableName: tableName || 'output',
    columns: [],
  };

  // Get field names from the output table
  const fieldNames = outputTable.schema.fields.map((f) => f.name);

  // Generate column schemas using Arrow's type information and sample data
  newSchema.columns = fieldNames.map((fieldName) => {
    // Get field from Arrow schema
    const field = outputTable.schema.fields.find((f) => f.name === fieldName);

    // Determine type information
    let type = 'unknown';
    let nullable = true;

    if (field) {
      // Map Arrow type to string representation
      type = mapArrowTypeToString(field.type);
      nullable = field.nullable;
    } else if (sampleOutput) {
      // Use sample output to infer type if Arrow schema doesn't have it
      const value = sampleOutput[fieldName];
      type = inferTypeFromValue(value);
      nullable = value === null || value === undefined;
    }

    return {
      name: fieldName,
      type,
      nullable,
    };
  });

  return newSchema;
}

/**
 * Helper method to infer type from a JavaScript value
 * @param value Any JavaScript value
 * @returns String representation of the type
 */
export function inferTypeFromValue(value: any): string {
  if (value === null || value === undefined) {
    return 'unknown';
  }

  // Check primitive types
  if (typeof value === 'string') return 'string';
  if (typeof value === 'number') {
    // Check if integer or float
    return Number.isInteger(value) ? 'integer' : 'float';
  }
  if (typeof value === 'boolean') return 'boolean';
  if (value instanceof Date) return 'date';

  // Check array types
  if (Array.isArray(value)) {
    if (value.length > 0) {
      // Try to infer element type from first element
      const elementType = inferTypeFromValue(value[0]);
      return `list<${elementType}>`;
    }
    return 'list<unknown>';
  }

  // Check object types
  if (typeof value === 'object') {
    // Convert object structure to a struct type
    const fields = Object.entries(value)
      .map(([key, val]) => `${key}: ${inferTypeFromValue(val)}`)
      .join(', ');
    return `struct<${fields}>`;
  }

  return 'unknown';
}

/**
 * Provides a detailed and accurate estimate of memory usage for the Arrow table
 * and its operations.
 *
 * @returns Detailed memory usage information in bytes
 */
export async function estimateMemoryUsage(
  table: Table<any>,
  pendingOpsLength: number,
): Promise<{
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
  // Initialize result structure
  let columnarDataBytes = 0;
  let metadataBytes = 0;

  // Analyze each column in the table
  for (const field of table.schema.fields) {
    const vector = table.getChild(field.name);
    if (!vector) continue;

    // Estimate memory based on data type and row count
    const numRows = vector.length;
    let bytesPerValue = 0;

    // Determine bytes per value based on data type
    switch (field.type.toString()) {
      // Numeric types
      case 'Int8':
      case 'Uint8':
        bytesPerValue = 1;
        break;
      case 'Int16':
      case 'Uint16':
        bytesPerValue = 2;
        break;
      case 'Int32':
      case 'Uint32':
      case 'Float32':
        bytesPerValue = 4;
        break;
      case 'Int64':
      case 'Uint64':
      case 'Float64':
      case 'Date':
      case 'Timestamp':
        bytesPerValue = 8;
        break;

      // Boolean (bit-packed, 1 bit per value + overhead)
      case 'Bool':
        bytesPerValue = 0.125; // 1/8 byte per boolean
        break;

      // String (variable size)
      case 'Utf8':
        // Get a sample of string values to estimate average length
        let totalStringLength = 0;
        const sampleSize = Math.min(100, numRows);
        let stringSamples = 0;

        for (let i = 0; i < sampleSize; i++) {
          const sampleIndex = Math.floor(i * (numRows / sampleSize));
          const value = vector.get(sampleIndex);
          if (typeof value === 'string') {
            // Estimate 2 bytes per character (UTF-16) + overhead
            totalStringLength += value.length * 2 + 24; // 24 bytes for JS string overhead
            stringSamples++;
          }
        }

        // Calculate average bytes per string
        bytesPerValue =
          stringSamples > 0 ? totalStringLength / stringSamples : 16; // Default if no samples
        break;

      // Binary data
      case 'Binary':
        bytesPerValue = 32; // Rough estimate for binary data
        break;

      // Lists and complex types
      case 'List':
      case 'Struct':
      case 'Map':
        // These are more complex - try to estimate deeper
        try {
          // For nested types, try to get a better estimate by looking at children
          const sampleObj = vector.get(0);
          if (sampleObj && typeof sampleObj === 'object') {
            const jsonSize = JSON.stringify(sampleObj).length * 2; // Rough approximation
            bytesPerValue = jsonSize + 40; // Add overhead
          } else {
            bytesPerValue = 64; // Fallback for complex types
          }
        } catch (e) {
          bytesPerValue = 64; // Fallback for complex types
        }
        break;

      // Default for any other types
      default:
        bytesPerValue = 16; // General fallback
        break;
    }

    // Calculate total for this column (data + validity bitmap)
    const validityBitmapBytes = Math.ceil(numRows / 8); // 1 bit per row for null tracking
    const columnDataBytes = numRows * bytesPerValue;
    const columnTotalBytes = columnDataBytes + validityBitmapBytes;

    // Add to totals
    columnarDataBytes += columnTotalBytes;

    // Estimate field metadata size
    metadataBytes += field.name.length * 2 + 40; // Field name + type info overhead
  }

  // Schema metadata overhead
  metadataBytes += table.schema.fields.length * 16 + 100;

  // Pending operations overhead
  const estimatedOperationOverhead = pendingOpsLength * 512; // 512 bytes per operation as estimate

  // Calculate total
  const tableBytes = columnarDataBytes + metadataBytes;
  const totalEstimatedBytes = tableBytes + estimatedOperationOverhead;

  // Helper function to format bytes to human-readable string
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';

    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));

    return (
      Number.parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i]
    );
  };

  // Return comprehensive memory usage information
  return {
    tableBytes,
    columnarDataBytes,
    metadataBytes,
    pendingOperations: pendingOpsLength,
    estimatedOperationOverhead,
    totalEstimatedBytes,
    humanReadable: {
      total: formatBytes(totalEstimatedBytes),
      table: formatBytes(tableBytes),
      columnarData: formatBytes(columnarDataBytes),
      metadata: formatBytes(metadataBytes),
      operations: formatBytes(estimatedOperationOverhead),
    },
  };
}
