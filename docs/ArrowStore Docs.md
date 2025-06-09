# ArrowStore API Documentation

## Overview

ArrowStore is a high-performance data store implementation using Apache Arrow Tables with vectorized operations for improved performance and memory efficiency. It provides a comprehensive API for data manipulation, filtering, sorting, and aggregation operations with lazy evaluation.

## Core Features

- **Lazy Evaluation**: Operations are queued and only executed when data is actually needed
- **Vectorized Operations**: Optimized for performance using Apache Arrow's columnar memory format
- **Memory Efficiency**: Batch processing and smart memory management for large datasets
- **Comprehensive Query API**: Rich set of operations for filtering, transforming, and analyzing data
- **SQL-like Capabilities**: Support for SQL-like filtering and queries

## Installation

```bash
npm install arrow-store
```

## Basic Usage

```typescript
import { ArrowStore, Aggregations } from 'arrow-store';
import { tableFromArrays } from 'apache-arrow';

// Create Arrow table from data
const data = {
  id: [1, 2, 3, 4, 5],
  name: ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
  age: [25, 30, 35, 40, 45],
  department: ['Engineering', 'Product', 'Engineering', 'HR', 'Product']
};

const table = tableFromArrays(data);

// Create ArrowStore instance
const store = new ArrowStore(table);

// Chain operations (these are lazily evaluated)
const result = await store
  .filter([
    { field: 'age', filter: { op: 'gte', value: 30 } }
  ])
  .sort([{ field: 'name', direction: 'asc' }])
  .getAll();

console.log(result);
```

## Constructor

### `new ArrowStore<T>(table, schema?, options?)`

Creates a new ArrowStore instance.

**Parameters:**
- `table`: Arrow Table - The table containing the data
- `schema?`: TableSchema - Optional schema definition
- `options?`: ArrowStoreOptions - Optional configuration options

**Type Parameters:**
- `T`: Record<string, any> - Type of the row objects

**Example:**
```typescript
const store = new ArrowStore(table);
```

## Core Methods

### Data Retrieval

#### `getAll(): Promise<T[]>`

Returns all data from the store.

**Returns:** Promise resolving to an array of row objects

**Example:**
```typescript
const allData = await store.getAll();
```

#### `count(): Promise<number>`

Counts the number of rows in the store.

**Returns:** Promise resolving to the number of rows

**Example:**
```typescript
const rowCount = await store.count();
```

#### `getSource(): Table`

Gets the underlying Apache Arrow Table.

**Returns:** The Apache Arrow Table

**Example:**
```typescript
const arrowTable = store.getSource();
```

### Filtering

#### `filter<R extends T = T>(filters: FilterCondition<T>[]): ArrowStore<R>`

Filters the data based on the provided filter conditions.

**Parameters:**
- `filters`: Array of filter conditions to apply

**Returns:** A new ArrowStore instance with filtered data

**Example:**
```typescript
const filteredStore = store.filter([
  { field: 'age', filter: { op: 'gte', value: 30 } },
  { 
    OR: [
      { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
      { field: 'department', filter: { op: 'eq', value: 'Product' } }
    ]
  }
]);
```

#### `filterSql<R extends T = T>(sqlFilter: string): ArrowStore<R>`

Filters data using a SQL-like WHERE clause.

**Parameters:**
- `sqlFilter`: SQL-like WHERE clause string (without the "WHERE" keyword)

**Returns:** A new ArrowStore instance with filtered data

**Example:**
```typescript
const filteredStore = store.filterSql(
  "age >= 30 AND department IN ('Engineering', 'Product')"
);
```

#### `filterEquals<R extends T = T>(simpleFilters: Partial<T>): ArrowStore<R>`

Filters data with simple field-value equality pairs.

**Parameters:**
- `simpleFilters`: Object where keys are field names and values are what to match

**Returns:** A new ArrowStore instance with filtered data

**Example:**
```typescript
const filteredStore = store.filterEquals({
  department: 'Engineering',
  active: true
});
```

### Transformation

#### `select<K extends keyof T>(fields: K[]): ArrowStore<Pick<T, K>>`

Selects specific fields/columns.

**Parameters:**
- `fields`: Array of field names to select

**Returns:** A new ArrowStore instance with only the selected fields

**Example:**
```typescript
const nameAndAgeStore = store.select(['name', 'age']);
```

#### `map<R extends Record<string, any>>(mapFn: (item: T) => R, resultSchema?: TableSchema): ArrowStore<R>`

Maps each row to a new object structure.

**Parameters:**
- `mapFn`: Function to transform each row
- `resultSchema?`: Optional schema for the transformed data

**Returns:** A new ArrowStore instance with mapped data

**Example:**
```typescript
const mappedStore = store.map(person => ({
  fullName: `${person.firstName} ${person.lastName}`,
  birthYear: new Date().getFullYear() - person.age
}));
```

### Sorting and Pagination

#### `sort(options: SortOptions[]): ArrowStore<T>`

Sorts the data based on one or more fields.

**Parameters:**
- `options`: Array of sort configurations with field and direction

**Returns:** A new ArrowStore instance with sorted data

**Example:**
```typescript
const sortedStore = store.sort([
  { field: 'age', direction: 'desc' },
  { field: 'name', direction: 'asc' }
]);
```

#### `paginate(page: number, pageSize: number): ArrowStore<T>`

Paginates data with the specified page and page size.

**Parameters:**
- `page`: Page number (1-based)
- `pageSize`: Number of items per page

**Returns:** A new ArrowStore instance with paginated data

**Example:**
```typescript
const pageTwo = store.paginate(2, 10); // Second page with 10 items per page
```

#### `slice(start: number, end: number): ArrowStore<T>`

Returns a slice of the data from start to end.

**Parameters:**
- `start`: Start index (inclusive)
- `end`: End index (exclusive)

**Returns:** A new ArrowStore instance with the sliced data

**Example:**
```typescript
const slicedStore = store.slice(10, 20); // Items 10-19
```

### Aggregation

#### `groupBy<K extends keyof T, R extends Record<string, any>>(field: K, aggregations: Record<string, (values: any[]) => any>): ArrowStore<R>`

Groups data by a field and computes aggregations.

**Parameters:**
- `field`: Field to group by
- `aggregations`: Object mapping output field names to aggregation functions

**Returns:** A new ArrowStore instance with grouped and aggregated data

**Example:**
```typescript
const departmentStats = store.groupBy('department', {
  count: Aggregations.count(),
  avgAge: Aggregations.avg('age'),
  totalSalary: Aggregations.sum('salary')
});

// Result structure example:
// [
//   { department: 'Engineering', count: 2, avgAge: 30, totalSalary: 200000 },
//   { department: 'Product', count: 2, avgAge: 37.5, totalSalary: 220000 },
//   { department: 'HR', count: 1, avgAge: 40, totalSalary: 90000 }
// ]
```

### Reduction Operations

#### `reduce<R>(reducer: (accumulator: R, current: T, index: number) => R, initialValue: R): Promise<R>`

Reduces the data to a single value.

**Parameters:**
- `reducer`: Function to apply to each row with an accumulator
- `initialValue`: Initial value for the accumulator

**Returns:** Promise resolving to the accumulated result

**Example:**
```typescript
const totalAge = await store.reduce(
  (sum, person, index) => sum + person.age, 
  0
);
```

#### `fold<R>(folder: (accumulator: R, current: T, index: number) => R, initialValue: R): Promise<R>`

Alias for `reduce`.

#### `toSet<K>(keyFn?: (item: T) => K): Promise<Set<K | T>>`

Converts the data to a Set with optional key extraction.

**Parameters:**
- `keyFn?`: Optional function to extract a key from each row

**Returns:** Promise resolving to a Set of unique values

**Example:**
```typescript
// Get unique departments
const departments = await store.toSet(person => person.department);
```

#### `toMap<K, V = T>(keyFn: (item: T) => K, valueFn?: (item: T) => V): Promise<Map<K, V>>`

Converts the data to a Map with keys and values extracted from rows.

**Parameters:**
- `keyFn`: Function to extract a key from each row
- `valueFn?`: Optional function to transform each row into a value

**Returns:** Promise resolving to a Map

**Example:**
```typescript
// Create a map of id -> name
const idToName = await store.toMap(
  person => person.id,
  person => person.name
);
```

## Memory Management

#### `flush(): Promise<ArrowStore<T>>`

Executes all pending operations and returns a new store.

**Returns:** Promise resolving to a new ArrowStore with computed results

**Example:**
```typescript
const computedStore = await store.flush();
```

#### `estimateMemoryUsage(): Promise<MemoryUsageInfo>`

Estimates memory usage of the store.

**Returns:** Promise resolving to detailed memory usage information

**Example:**
```typescript
const memoryInfo = await store.estimateMemoryUsage();
console.log(`Total estimated memory: ${memoryInfo.humanReadable.total}`);
```

## Column Operations

#### `sumColumn(columnName: string): Promise<number>`

Computes the sum of values in a numeric column.

**Parameters:**
- `columnName`: Name of the column to sum

**Returns:** Promise resolving to the sum

**Example:**
```typescript
const totalSalary = await store.sumColumn('salary');
```

#### `averageColumn(columnName: string): Promise<number | null>`

Computes the average of values in a numeric column.

**Parameters:**
- `columnName`: Name of the column to average

**Returns:** Promise resolving to the average or null if no data

**Example:**
```typescript
const avgAge = await store.averageColumn('age');
```

#### `minMaxColumn(columnName: string): Promise<{ min: any; max: any }>`

Finds the minimum and maximum values in a column.

**Parameters:**
- `columnName`: Name of the column to analyze

**Returns:** Promise resolving to an object with min and max values

**Example:**
```typescript
const { min, max } = await store.minMaxColumn('age');
console.log(`Age range: ${min} - ${max}`);
```

#### `countDistinct(columnName: string): Promise<number>`

Counts distinct values in a column.

**Parameters:**
- `columnName`: Name of the column to analyze

**Returns:** Promise resolving to the count of distinct values

**Example:**
```typescript
const departmentCount = await store.countDistinct('department');
```

## Advanced Usage Examples

### Chaining Operations

```typescript
const results = await store
  .filter([{ field: 'active', filter: { op: 'eq', value: true } }])
  .select(['id', 'name', 'department', 'salary'])
  .sort([{ field: 'salary', direction: 'desc' }])
  .slice(0, 10)
  .getAll();
```

### Complex Filtering

```typescript
import { and, or, not, field, gt, eq, inArray } from 'arrow-store';

const filtered = store.filter([
  and([
    field('age', gt(30)),
    or([
      field('department', eq('Engineering')),
      field('department', eq('Product'))
    ]),
    not(field('isContractor', eq(true)))
  ])
]);
```

### Using SQL-like Filters

```typescript
const filtered = store.filterSql(
  "age > 30 AND department IN ('Engineering', 'Product') AND NOT isContractor = true"
);
```

### Grouping and Aggregation

```typescript
const stats = await store
  .groupBy('department', {
    count: Aggregations.count(),
    avgAge: Aggregations.avg('age'),
    minSalary: Aggregations.min('salary'),
    maxSalary: Aggregations.max('salary')
  })
  .sort([{ field: 'count', direction: 'desc' }])
  .getAll();
```

### Processing Large Datasets Efficiently

```typescript
// For very large datasets, use batch processing patterns
const store = new ArrowStore(largeTable);

// Use aggregations rather than loading all data
const summary = {
  total: await store.count(),
  averageAge: await store.averageColumn('age'),
  departmentCounts: await store
    .groupBy('department', { count: Aggregations.count() })
    .getAll()
};

// Only retrieve necessary data
const topEmployees = await store
  .sort([{ field: 'performance', direction: 'desc' }])
  .slice(0, 100)  // Only get top 100
  .select(['id', 'name', 'performance'])  // Only select needed fields
  .getAll();
```

## Type Definitions

For complete type definitions, refer to the source code or TypeScript declaration files.