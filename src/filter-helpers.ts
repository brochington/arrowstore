// Enhanced filter types for improved type safety

// Basic filter operations
export type ComparisonOperator = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';
export type StringOperator = 'contains' | 'startsWith' | 'endsWith';
export type ArrayOperator = 'in';
export type FilterOperator =
  | ComparisonOperator
  | StringOperator
  | ArrayOperator;

// Type-safe filter condition interfaces
export interface ComparisonFilter<T> {
  op: ComparisonOperator;
  value: T;
}

export interface StringFilter {
  op: StringOperator;
  value: string;
}

export interface ArrayFilter<T> {
  op: ArrayOperator;
  value: T[];
}

// Union type for all possible basic filter types
export type BasicFilter<T> =
  | ComparisonFilter<T>
  | StringFilter
  | ArrayFilter<T>;

// Complex filter condition interfaces
export interface AndFilter<T extends Record<string, any>> {
  AND: FilterCondition<T>[];
}

export interface OrFilter<T extends Record<string, any>> {
  OR: FilterCondition<T>[];
}

export interface NotFilter<T extends Record<string, any>> {
  NOT: FilterCondition<T>;
}

export interface FieldFilter<T extends Record<string, any>> {
  field: keyof T;
  filter: BasicFilter<any>;
}

// Union type for all possible filter conditions
export type FilterCondition<T extends Record<string, any>> =
  | AndFilter<T>
  | OrFilter<T>
  | NotFilter<T>
  | FieldFilter<T>;

// Type-safe filters object
export type Filters<T extends Record<string, any>> = FilterCondition<T>[];

// Helper function to create a field filter
export function field<T extends Record<string, any>, K extends keyof T>(
  fieldName: K,
  filter: BasicFilter<T[K]>,
): FieldFilter<T> {
  return { field: fieldName, filter };
}

// Helper functions to create complex filters
export function and<T extends Record<string, any>>(
  conditions: FilterCondition<T>[],
): AndFilter<T> {
  return { AND: conditions };
}

export function or<T extends Record<string, any>>(
  conditions: FilterCondition<T>[],
): OrFilter<T> {
  return { OR: conditions };
}

export function not<T extends Record<string, any>>(
  condition: FilterCondition<T>,
): NotFilter<T> {
  return { NOT: condition };
}

// Helper functions to create basic filter operations
export function eq<T>(value: T): ComparisonFilter<T> {
  return { op: 'eq', value };
}

export function neq<T>(value: T): ComparisonFilter<T> {
  return { op: 'neq', value };
}

export function gt<T>(value: T): ComparisonFilter<T> {
  return { op: 'gt', value };
}

export function gte<T>(value: T): ComparisonFilter<T> {
  return { op: 'gte', value };
}

export function lt<T>(value: T): ComparisonFilter<T> {
  return { op: 'lt', value };
}

export function lte<T>(value: T): ComparisonFilter<T> {
  return { op: 'lte', value };
}

export function contains(value: string): StringFilter {
  return { op: 'contains', value };
}

export function startsWith(value: string): StringFilter {
  return { op: 'startsWith', value };
}

export function endsWith(value: string): StringFilter {
  return { op: 'endsWith', value };
}

export function inArray<T>(values: T[]): ArrayFilter<T> {
  return { op: 'in', value: values };
}
