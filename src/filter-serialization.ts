import type {
  AndFilter,
  FieldFilter,
  FilterCondition,
  FilterOperator,
  NotFilter,
  OrFilter,
} from './filter-helpers';

// Filter serialization and deserialization utilities

/**
 * JSON representation of filter conditions for serialization
 */
export type SerializedBasicFilter = {
  op: FilterOperator;
  value: any;
};

export type SerializedFieldFilter = {
  type: 'field';
  field: string;
  filter: SerializedBasicFilter;
};

export type SerializedAndFilter = {
  type: 'and';
  conditions: SerializedFilterCondition[];
};

export type SerializedOrFilter = {
  type: 'or';
  conditions: SerializedFilterCondition[];
};

export type SerializedNotFilter = {
  type: 'not';
  condition: SerializedFilterCondition;
};

export type SerializedFilterCondition =
  | SerializedFieldFilter
  | SerializedAndFilter
  | SerializedOrFilter
  | SerializedNotFilter;

/**
 * Serialize a filter condition tree to a JSON-compatible object
 * @param filter The filter condition to serialize
 * @returns A JSON-serializable representation of the filter
 */
export function serializeFilter<T extends Record<string, any>>(
  filter: FilterCondition<T>,
): SerializedFilterCondition {
  // Check filter type and serialize accordingly
  if ('field' in filter && 'filter' in filter) {
    // Field filter
    return {
      type: 'field',
      field: filter.field as string,
      filter: {
        op: filter.filter.op,
        value: filter.filter.value,
      },
    };
  } else if ('AND' in filter) {
    // AND filter
    return {
      type: 'and',
      conditions: filter.AND.map((condition) => serializeFilter(condition)),
    };
  } else if ('OR' in filter) {
    // OR filter
    return {
      type: 'or',
      conditions: filter.OR.map((condition) => serializeFilter(condition)),
    };
  } else if ('NOT' in filter) {
    // NOT filter
    return {
      type: 'not',
      condition: serializeFilter(filter.NOT),
    };
  }

  throw new Error(`Unknown filter type: ${JSON.stringify(filter)}`);
}

/**
 * Serialize an array of filter conditions to a JSON-compatible array
 * @param filters Array of filter conditions to serialize
 * @returns A JSON-serializable representation of the filters
 */
export function serializeFilters<T extends Record<string, any>>(
  filters: FilterCondition<T>[],
): SerializedFilterCondition[] {
  return filters.map((filter) => serializeFilter(filter));
}

/**
 * Deserialize a JSON representation of a filter into a filter condition
 * @param serialized The serialized filter condition
 * @returns A filter condition object
 */
export function deserializeFilter<T extends Record<string, any>>(
  serialized: SerializedFilterCondition,
): FilterCondition<T> {
  switch (serialized.type) {
    case 'field': {
      // Deserialize field filter
      const { field, filter } = serialized;
      return {
        field: field as keyof T,
        filter: {
          op: filter.op,
          value: filter.value,
        },
      } as FieldFilter<T>;
    }

    case 'and': {
      // Deserialize AND filter
      return {
        AND: serialized.conditions.map((condition) =>
          deserializeFilter<T>(condition),
        ),
      } as AndFilter<T>;
    }

    case 'or': {
      // Deserialize OR filter
      return {
        OR: serialized.conditions.map((condition) =>
          deserializeFilter<T>(condition),
        ),
      } as OrFilter<T>;
    }

    case 'not': {
      // Deserialize NOT filter
      return {
        NOT: deserializeFilter<T>(serialized.condition),
      } as NotFilter<T>;
    }

    default:
      throw new Error(
        `Unknown serialized filter type: ${JSON.stringify(serialized)}`,
      );
  }
}

/**
 * Deserialize a JSON array of filter conditions
 * @param serialized Array of serialized filter conditions
 * @returns Array of filter condition objects
 */
export function deserializeFilters<T extends Record<string, any>>(
  serialized: SerializedFilterCondition[],
): FilterCondition<T>[] {
  return serialized.map((condition) => deserializeFilter<T>(condition));
}

/**
 * Convert filter conditions to a JSON string
 * @param filters The filter conditions to stringify
 * @returns JSON string representation of the filters
 */
export function filtersToJson<T extends Record<string, any>>(
  filters: FilterCondition<T>[],
): string {
  return JSON.stringify(serializeFilters(filters));
}

/**
 * Parse filter conditions from a JSON string
 * @param json JSON string representation of filters
 * @returns Array of filter condition objects
 */
export function filtersFromJson<T extends Record<string, any>>(
  json: string,
): FilterCondition<T>[] {
  const parsed = JSON.parse(json) as SerializedFilterCondition[];
  return deserializeFilters<T>(parsed);
}

/**
 * Create a filter from a plain JavaScript object (useful for API endpoints)
 * @param obj Plain JavaScript object representing a filter
 * @returns A filter condition
 */
export function filterFromObject<T extends Record<string, any>>(
  obj: any,
): FilterCondition<T> {
  // Validate the object has the required properties
  if (!obj || typeof obj !== 'object') {
    throw new Error('Filter must be an object');
  }

  // Convert the object to a serialized filter condition
  if ('type' in obj) {
    // Already in serialized format
    return deserializeFilter<T>(obj as SerializedFilterCondition);
  }

  // Try to infer the filter type
  if ('field' in obj && 'filter' in obj) {
    return {
      field: obj.field,
      filter: obj.filter,
    } as FieldFilter<T>;
  }

  if ('AND' in obj) {
    return {
      AND: obj.AND.map((c: any) => filterFromObject<T>(c)),
    } as AndFilter<T>;
  }

  if ('OR' in obj) {
    return {
      OR: obj.OR.map((c: any) => filterFromObject<T>(c)),
    } as OrFilter<T>;
  }

  if ('NOT' in obj) {
    return {
      NOT: filterFromObject<T>(obj.NOT),
    } as NotFilter<T>;
  }

  throw new Error(`Cannot convert object to filter: ${JSON.stringify(obj)}`);
}

/**
 * Create filters from a plain JavaScript array (useful for API endpoints)
 * @param arr Array of plain JavaScript objects representing filters
 * @returns Array of filter conditions
 */
export function filtersFromArray<T extends Record<string, any>>(
  arr: any[],
): FilterCondition<T>[] {
  if (!Array.isArray(arr)) {
    throw new Error('Filters must be an array');
  }

  return arr.map((obj) => filterFromObject<T>(obj));
}
