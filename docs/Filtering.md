# ArrowStore Filtering Guide

This guide explains how to use the enhanced filtering capabilities in ArrowStore. The new filtering system provides type safety, supports multiple conditions on the same field, and enables complex logical expressions (AND, OR, NOT).

## Table of Contents

- [Basic Concepts](#basic-concepts)
- [Filter Types](#filter-types)
- [Simple Filtering](#simple-filtering)
- [Complex Filtering](#complex-filtering)
- [Helper Functions](#helper-functions)
- [Building Dynamic Filters](#building-dynamic-filters)
- [Serialization and Deserialization](#serialization-and-deserialization)
- [Performance Considerations](#performance-considerations)
- [Type Safety](#type-safety)
- [Examples](#examples)

## Basic Concepts

The filtering system is built around a few core concepts:

1. **Filter Conditions**: Individual operations like equality, comparison, etc.
2. **Logical Operations**: AND, OR, and NOT operations to combine conditions
3. **Field Targeting**: Explicitly specifying which field a condition applies to
4. **Type Safety**: Filter conditions that respect the data types of your fields

## Filter Types

### Basic Filters

These operate on individual field values:

- **Comparison**: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
- **String Operations**: `contains`, `startsWith`, `endsWith`
- **Array Operations**: `in` (check if value exists in an array)

### Complex Filters

These combine multiple filter conditions:

- **AND**: All conditions must be true
- **OR**: At least one condition must be true
- **NOT**: Negates a condition

## Simple Filtering

The most basic form of filtering applies a single condition to a single field.

```typescript
// Get users who are exactly 30 years old
store.filter([
  field('age', eq(30))
])
```

Multiple filters can be applied as an implicit AND operation:

```typescript
// Get active users in the Engineering department
store.filter([
  field('isActive', eq(true)),
  field('department', eq('Engineering'))
])
```

## Complex Filtering

### AND Operations

Combine multiple conditions with AND logic:

```typescript
// Get users between 25 and 35 years old
store.filter([
  and([
    field('age', gt(25)),
    field('age', lt(35))
  ])
])
```

### OR Operations

Apply OR logic to conditions:

```typescript
// Get users in either Engineering or Product departments
store.filter([
  or([
    field('department', eq('Engineering')),
    field('department', eq('Product'))
  ])
])
```

### NOT Operations

Negate conditions:

```typescript
// Get users who are not in Sales
store.filter([
  not(field('department', eq('Sales')))
])
```

### Nested Operations

Complex conditions can be nested to any depth:

```typescript
// Get active engineers OR active product managers
store.filter([
  field('isActive', eq(true)),
  or([
    field('department', eq('Engineering')),
    and([
      field('department', eq('Product')),
      field('title', contains('Manager'))
    ])
  ])
])
```

## Helper Functions

The filtering system provides helper functions to make filter creation more readable:

### Field Selection

- `field(fieldName, filterCondition)` - Apply a filter to a specific field

### Comparison Operators

- `eq(value)` - Equal to value
- `neq(value)` - Not equal to value
- `gt(value)` - Greater than value
- `gte(value)` - Greater than or equal to value
- `lt(value)` - Less than value
- `lte(value)` - Less than or equal to value

### String Operators

- `contains(value)` - Field contains string
- `startsWith(value)` - Field starts with string
- `endsWith(value)` - Field ends with string

### Array Operators

- `inArray(values)` - Field value is in the array

### Logical Operators

- `and(conditions)` - All conditions must be true
- `or(conditions)` - At least one condition must be true
- `not(condition)` - Negate a condition

## Building Dynamic Filters

The filter system is designed to work well with dynamically built queries:

```typescript
function buildUserFilter(options: {
  isActive?: boolean;
  minAge?: number;
  maxAge?: number;
  departments?: string[];
  searchTerm?: string;
}) {
  const filters = [];
  
  // Add filters based on options
  if (options.isActive !== undefined) {
    filters.push(field('isActive', eq(options.isActive)));
  }
  
  // Age range filter
  const ageFilters = [];
  if (options.minAge !== undefined) {
    ageFilters.push(field('age', gte(options.minAge)));
  }
  if (options.maxAge !== undefined) {
    ageFilters.push(field('age', lte(options.maxAge)));
  }
  if (ageFilters.length > 0) {
    filters.push(and(ageFilters));
  }
  
  // Department filter
  if (options.departments && options.departments.length > 0) {
    filters.push(field('department', inArray(options.departments)));
  }
  
  // Search filter
  if (options.searchTerm) {
    filters.push(
      or([
        field('name', contains(options.searchTerm)),
        field('email', contains(options.searchTerm)),
        field('department', contains(options.searchTerm))
      ])
    );
  }
  
  return filters;
}

// Usage
const userFilters = buildUserFilter({
  isActive: true,
  minAge: 25,
  maxAge: 40,
  departments: ['Engineering', 'Product'],
  searchTerm: 'manager'
});

const filteredUsers = await store.filter(userFilters).getAll();
```

## Performance Considerations

The filter implementation is optimized for performance:

1. **Vectorized Operations**: Uses Arrow's columnar structure for efficiency
2. **Batch Processing**: Processes rows in batches for better cache locality
3. **Short-Circuit Evaluation**: Skips already filtered rows
4. **Optimized Paths**: Special optimizations for common operations like equality checks
5. **Lazy Evaluation**: Filters are applied only when data is actually requested

For best performance:

- Apply the most restrictive filters first
- Use equality filters when possible (they have special optimizations)
- Be mindful of complex OR conditions, which can be more expensive

## Type Safety

The filter system is fully type-safe, meaning your IDE and TypeScript compiler will catch many potential errors:

```typescript
interface User {
  id: number;
  name: string;
  age: number;
}

// This works fine
store.filter([field('age', gt(25))])

// TypeScript error - 'title' is not a field in User
store.filter([field('title', eq('Manager'))])

// TypeScript error - age is a number, not a string
store.filter([field('age', contains('25'))])
```

## ArrowStore Filter Methods

The ArrowStore class provides several methods to work with different filter formats:

### Standard Filter Method

The core filter method using the new filter condition format:

```typescript
// Using the structured filter conditions
store.filter([
  field('isActive', eq(true)),
  field('department', eq('Engineering'))
]);
```

### Filtering with JSON

Filter using a JSON string representing serialized filter conditions:

```typescript
// JSON string of serialized filters
const filtersJson = `[
  {"type":"field","field":"isActive","filter":{"op":"eq","value":true}},
  {"type":"field","field":"department","filter":{"op":"eq","value":"Engineering"}}
]`;

// Apply JSON filters directly
store.filterFromJson(filtersJson);
```

### Legacy Filter Support

For backward compatibility with the old filter format:

```typescript
// Old filter format
const legacyFilters = {
  isActive: { op: 'eq', value: true },
  age: { op: 'gt', value: 30 }
};

// Use with new method
store.filterLegacy(legacyFilters);
```

### Simple Equality Filtering

A concise way to filter when all conditions are simple equality checks:

```typescript
// Simple field-value pairs
store.filterEquals({
  department: 'Engineering',
  isActive: true
});
```

### URL Query String Filtering

Filter directly from URL query parameters:

```typescript
// URL query string
const queryString = 'department=Engineering,Product&minAge=25&maxAge=40&isActive=true';

// Apply query string filters
store.filterFromQueryString(queryString);
```

### Serialized Filter Objects

Filter using serialized filter objects:

```typescript
// Array of serialized filter objects
const serializedFilters = [
  {
    type: 'field',
    field: 'isActive',
    filter: { op: 'eq', value: true }
  },
  {
    type: 'field',
    field: 'department',
    filter: { op: 'in', value: ['Engineering', 'Product'] }
  }
];

// Apply serialized filters
store.filterFromSerialized(serializedFilters);
```

### Utility Methods

Helper methods for working with filters:

```typescript
// Serialize filters to JSON
const filters = [field('isActive', eq(true))];
const jsonString = store.serializeFilters(filters);

// Convert filters to URL query string
const queryString = store.filtersToQueryString(filters);
```

## Serialization and Deserialization

The filter system supports serialization to and deserialization from JSON, enabling you to:

- Store filters in databases or local storage
- Transmit filters over network requests
- Share filters between components or applications
- Reconstruct filters from external sources

### Basic Serialization

To convert filters to a JSON string:

```typescript
import { filtersToJson, filtersFromJson } from './filter-serialization';

// Create filters
const filters = [
  field('isActive', eq(true)),
  field('department', eq('Engineering'))
];

// Serialize to JSON string
const jsonString = filtersToJson(filters);
console.log(jsonString);
// Output: '[{"type":"field","field":"isActive","filter":{"op":"eq","value":true}},{"type":"field","field":"department","filter":{"op":"eq","value":"Engineering"}}]'

// Later, deserialize from JSON string
const deserializedFilters = filtersFromJson(jsonString);
// Now you can use deserializedFilters with store.filter()
```

### Working with Plain Objects

You can also work with plain JavaScript objects:

```typescript
import { serializeFilters, deserializeFilters } from './filter-serialization';

// Create filters
const filters = [
  field('salary', gt(100000)),
  or([
    field('department', eq('Engineering')),
    field('department', eq('Product'))
  ])
];

// Serialize to plain objects
const serialized = serializeFilters(filters);
// serialized can be stored or transmitted as JSON

// Later, deserialize from objects
const deserialized = deserializeFilters(serialized);
```

### API Integration

When receiving filters from external sources:

```typescript
import { filtersFromJson, filterFromObject } from './filter-serialization';

// From API response body as JSON
function handleApiResponse(responseJson) {
  const filters = filtersFromJson(responseJson);
  store.filter(filters).getAll().then(results => {
    // Process results
  });
}

// From request body as already-parsed object
function handleRequestBody(requestBody) {
  if (Array.isArray(requestBody.filters)) {
    const filters = requestBody.filters.map(f => filterFromObject(f));
    // Use filters
  }
}
```

### Saving User Preferences

Store user's filter preferences:

```typescript
// Save filters to localStorage
function saveUserFilters(filters) {
  localStorage.setItem('userFilters', filtersToJson(filters));
}

// Load filters from localStorage
function loadUserFilters() {
  const json = localStorage.getItem('userFilters');
  if (json) {
    return filtersFromJson(json);
  }
  return []; // Default empty filters
}
```

### Serialized Filter Format

The serialized format for filters follows this structure:

```typescript
// Field filter
{
  "type": "field",
  "field": "fieldName",
  "filter": {
    "op": "eq", // or any other operator
    "value": "some value"
  }
}

// AND filter
{
  "type": "and",
  "conditions": [
    // array of serialized filter conditions
  ]
}

// OR filter
{
  "type": "or",
  "conditions": [
    // array of serialized filter conditions
  ]
}

// NOT filter
{
  "type": "not",
  "condition": {
    // a serialized filter condition
  }
}
```

## Examples

### Example 1: Simple Filtering

Find active users with a salary over $100,000:

```typescript
const highPaidActiveUsers = await store
  .filter([
    field('isActive', eq(true)),
    field('salary', gt(100000))
  ])
  .getAll();
```

### Example 2: Multiple Conditions on Same Field

Find users with age between 25 and 35:

```typescript
const youngAdults = await store
  .filter([
    and([
      field('age', gte(25)),
      field('age', lte(35))
    ])
  ])
  .getAll();
```

### Example 3: OR Conditions

Find users in specific departments:

```typescript
const techUsers = await store
  .filter([
    or([
      field('department', eq('Engineering')),
      field('department', eq('Product')),
      field('department', eq('Design'))
    ])
  ])
  .getAll();

// Alternative using inArray
const techUsers = await store
  .filter([
    field('department', inArray(['Engineering', 'Product', 'Design']))
  ])
  .getAll();
```

### Example 4: Complex Nested Conditions

Find active senior engineers or product managers with high salaries:

```typescript
const seniorTechStaff = await store
  .filter([
    field('isActive', eq(true)),
    field('salary', gt(120000)),
    or([
      and([
        field('department', eq('Engineering')),
        field('title', contains('Senior'))
      ]),
      and([
        field('department', eq('Product')),
        field('title', contains('Manager'))
      ])
    ])
  ])
  .getAll();
```

### Example 5: Combining with Other Operations

Apply filters along with sorting and pagination:

```typescript
const result = await store
  .filter([
    field('department', eq('Engineering')),
    field('salary', gt(100000))
  ])
  .sort([{ field: 'salary', direction: 'desc' }])
  .paginate(1, 20)
  .getAll();
```

### Example 6: Using NOT

Find users who are not in the Sales department:

```typescript
const nonSalesUsers = await store
  .filter([
    not(field('department', eq('Sales')))
  ])
  .getAll();
```

### Example 7: String Operations

Find users with email addresses from a specific domain:

```typescript
const companyUsers = await store
  .filter([
    field('email', endsWith('@company.com'))
  ])
  .getAll();
```

### Example 8: Complex Search

Implement a search across multiple fields:

```typescript
const searchTerm = 'smith';
const searchResults = await store
  .filter([
    or([
      field('name', contains(searchTerm)),
      field('email', contains(searchTerm)),
      field('notes', contains(searchTerm))
    ])
  ])
  .getAll();
```