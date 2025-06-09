import { Table, tableFromArrays } from 'apache-arrow';
import { ArrowStore } from '../src/ArrowStore';

interface TableSchema {
  tableName: string;
  columns: {
    name: string;
    type: string;
    nullable?: boolean;
  }[];
}

describe('ArrowStore SQL capabilities', () => {
  // Sample data for testing
  const sampleData = {
    id: [1, 2, 3, 4, 5],
    name: ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
    age: [25, 30, 35, 40, 45],
    department: [
      'Engineering',
      'Product',
      'Engineering',
      'Marketing',
      'Engineering',
    ],
    isActive: [true, true, false, true, false],
    salary: [75000, 85000, 90000, 70000, 95000],
  };

  // Sample schema
  const schema: TableSchema = {
    tableName: 'employees',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'string' },
      { name: 'age', type: 'integer' },
      { name: 'department', type: 'string' },
      { name: 'isActive', type: 'boolean' },
      { name: 'salary', type: 'integer' },
    ],
  };

  // Create a fresh ArrowStore before each test
  let store: ArrowStore<any>;

  beforeEach(() => {
    const table = tableFromArrays(sampleData);
    store = new ArrowStore<any>(table, schema);
  });

  describe('filterSql method', () => {
    it('should filter with basic equality condition', async () => {
      const filtered = store.filterSql("department = 'Engineering'");
      const results = await filtered.getAll();

      expect(results.length).toBe(3);
      expect(results.every((row) => row.department === 'Engineering')).toBe(
        true,
      );
    });

    it('should filter with numeric comparison', async () => {
      const filtered = store.filterSql('age > 30');
      const results = await filtered.getAll();

      expect(results.length).toBe(3);
      expect(results.every((row) => row.age > 30)).toBe(true);
    });

    it('should filter with multiple conditions using AND', async () => {
      const filtered = store.filterSql(
        "department = 'Engineering' AND isActive = true",
      );
      const results = await filtered.getAll();

      expect(results.length).toBe(1);
      expect(results[0].name).toBe('Alice');
    });

    it('should filter with multiple conditions using OR', async () => {
      const filtered = store.filterSql(
        "department = 'Marketing' OR department = 'Product'",
      );
      const results = await filtered.getAll();

      expect(results.length).toBe(2);
      expect(results[0].department).toBe('Product');
      expect(results[1].department).toBe('Marketing');
    });

    it('should filter with complex conditions using parentheses', async () => {
      const filtered = store.filterSql(
        "(department = 'Engineering' AND age < 30) OR (department = 'Marketing' AND isActive = true)",
      );
      const results = await filtered.getAll();

      expect(results.length).toBe(2);
      // Alice (Engineering, 25, active) and Dave (Marketing, 40, active)
      expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Dave']);
    });

    it('should filter with NOT condition', async () => {
      const filtered = store.filterSql("NOT department = 'Engineering'");
      const results = await filtered.getAll();

      expect(results.length).toBe(2);
      expect(results.every((row) => row.department !== 'Engineering')).toBe(
        true,
      );
    });

    it('should filter with IN operator', async () => {
      const filtered = store.filterSql(
        "department IN ('Engineering', 'Product')",
      );
      const results = await filtered.getAll();

      expect(results.length).toBe(4);
      expect(
        results.every((row) =>
          ['Engineering', 'Product'].includes(row.department),
        ),
      ).toBe(true);
    });

    it('should filter with LIKE operator for contains', async () => {
      const filtered = store.filterSql("name LIKE '%li%'");
      const results = await filtered.getAll();

      expect(results.length).toBe(2);
      expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie']);
    });

    it('should filter with LIKE operator for starts with', async () => {
      const filtered = store.filterSql("name LIKE 'A%'");
      const results = await filtered.getAll();

      expect(results.length).toBe(1);
      expect(results[0].name).toBe('Alice');
    });

    it('should filter with LIKE operator for ends with', async () => {
      const filtered = store.filterSql("name LIKE '%e'");
      const results = await filtered.getAll();

      expect(results.length).toBe(4);
      expect(results.map((r) => r.name).sort()).toEqual([
        'Alice',
        'Charlie',
        'Dave',
        'Eve',
      ]);
    });

    it('should handle different comparison operators', async () => {
      // Test >= operator
      let filtered = store.filterSql('age >= 35');
      let results = await filtered.getAll();
      expect(results.length).toBe(3);
      expect(results.every((row) => row.age >= 35)).toBe(true);

      // Test <= operator
      filtered = store.filterSql('age <= 30');
      results = await filtered.getAll();
      expect(results.length).toBe(2);
      expect(results.every((row) => row.age <= 30)).toBe(true);

      // Test != operator
      filtered = store.filterSql("department != 'Engineering'");
      results = await filtered.getAll();
      expect(results.length).toBe(2);
      expect(results.every((row) => row.department !== 'Engineering')).toBe(
        true,
      );
    });

    it('should handle boolean values correctly', async () => {
      const filtered = store.filterSql('isActive = true');
      const results = await filtered.getAll();

      expect(results.length).toBe(3);
      expect(results.every((row) => row.isActive === true)).toBe(true);
    });

    it('should throw error for invalid field names', async () => {
      expect(() => {
        store.filterSql("nonExistentField = 'value'");
      }).toThrow(/Field 'nonExistentField' does not exist/);
    });

    it('should throw error for syntax errors', async () => {
      expect(() => {
        store.filterSql('department = Engineering'); // Missing quotes
      }).toThrow(/Error parsing SQL filter/);
    });
  });

  describe('sql method', () => {
    it('should execute a basic SELECT query', async () => {
      const result = store.sql(
        "SELECT * FROM employees WHERE department = 'Engineering'",
      );
      const rows = await result.getAll();

      expect(rows.length).toBe(3);
      expect(rows.every((row) => row.department === 'Engineering')).toBe(true);
    });

    it('should execute a query with field projection', async () => {
      const result = store.sql(
        'SELECT name, age FROM employees WHERE age > 35',
      );
      const rows = await result.getAll();

      expect(rows.length).toBe(2);
      expect(Object.keys(rows[0]).sort()).toEqual(['age', 'name']);
      expect(rows.every((row) => row.age > 35)).toBe(true);
    });

    it('should execute a query with multiple conditions', async () => {
      const result = store.sql(
        "SELECT * FROM employees WHERE department = 'Engineering' AND isActive = true",
      );
      const rows = await result.getAll();

      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe('Alice');
    });

    it('should handle queries without WHERE clause', async () => {
      const result = store.sql('SELECT name, department FROM employees');
      const rows = await result.getAll();

      expect(rows.length).toBe(5);
      expect(Object.keys(rows[0]).sort()).toEqual(['department', 'name']);
    });

    it('should throw error for invalid SQL syntax', async () => {
      expect(() => {
        store.sql('INVALID QUERY');
      }).toThrow(/Only SELECT statements are supported/);
    });

    it('should throw error for missing FROM clause', async () => {
      expect(() => {
        store.sql("SELECT * WHERE department = 'Engineering'");
      }).toThrow(/FROM clause is required/);
    });
  });

  describe('complex SQL operations', () => {
    it('should handle complex queries with multiple conditions and projections', async () => {
      // For debugging purposes, let's break this down into parts
      // First, let's check if basic filtering works

      // const debugFilter = store.filterSql(
      //   "(age > 30 AND department = 'Engineering') OR (isActive = true AND salary > 80000)"
      // );
      // const _debugRows = await debugFilter.getAll();

      // console.log('DEBUG: Filtered rows', debugRows);

      // Now let's try the full SQL query
      const result = store.sql(`
        SELECT name, age, department 
        FROM employees 
        WHERE (age > 30 AND department = 'Engineering') OR (isActive = true AND salary > 80000)
      `);

      const rows = await result.getAll();

      // The correct expected results:
      // 1. Charlie (35, Engineering) - matches first condition
      // 2. Eve (45, Engineering) - matches first condition
      // 3. Bob (30, Product, active, 85000) - matches second condition

      expect(rows.length).toBe(3);
      expect(rows.map((r) => r.name).sort()).toEqual(['Bob', 'Charlie', 'Eve']);

      // Check if projection is working correctly
      if (rows.length > 0) {
        const keys = Object.keys(rows[0]).sort();
        // console.log('Projection fields', keys);
        expect(keys).toEqual(['age', 'department', 'name']);
      }
    });

    it('should chain SQL operations with other ArrowStore methods', async () => {
      // First filter with SQL, then sort and paginate
      const filtered = store.sql(
        'SELECT * FROM employees WHERE salary > 70000',
      );
      const sorted = filtered.sort([{ field: 'salary', direction: 'desc' }]);
      const paginated = sorted.paginate(1, 2);

      const rows = await paginated.getAll();

      expect(rows.length).toBe(2);
      expect(rows[0].salary).toBe(95000); // Eve has highest salary
      expect(rows[1].salary).toBe(90000); // Charlie has second highest
    });

    it('should handle subqueries using multiple method calls', async () => {
      // We can simulate a subquery by chaining calls
      const engineeringDept = store.filterSql("department = 'Engineering'");
      const highSalary = engineeringDept.filterSql('salary > 80000');
      const rows = await highSalary.getAll();

      expect(rows.length).toBe(2);
      expect(rows.map((r) => r.name).sort()).toEqual(['Charlie', 'Eve']);
    });
  });

  describe('error handling and edge cases', () => {
    it('should handle empty result sets gracefully', async () => {
      const filtered = store.filterSql("department = 'NonExistent'");
      const rows = await filtered.getAll();

      expect(rows.length).toBe(0);
    });

    it('should handle case sensitivity in SQL operators', async () => {
      // Test case insensitivity of SQL keywords
      const filtered = store.filterSql(
        "department = 'Engineering' and isActive = true",
      );
      const rows = await filtered.getAll();

      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe('Alice');
    });

    it('should validate fields before executing filters', async () => {
      expect(() => {
        store.filterSql("invalid_field = 'value'");
      }).toThrow(/Field 'invalid_field' does not exist/);
    });

    it('should handle extra whitespace in SQL queries', async () => {
      const filtered = store.filterSql(`
        department   =    'Engineering'
        AND   age   >   30
      `);
      const rows = await filtered.getAll();

      expect(rows.length).toBe(2);
      expect(rows.map((r) => r.name).sort()).toEqual(['Charlie', 'Eve']);
    });
  });

  describe('Debug tests for SQL implementation', () => {
    // This is a focused test to help debug the SQL method implementation
    it('should correctly parse and execute WHERE clauses', async () => {
      // Test simple conditions first
      let filtered = store.filterSql('age > 30');
      let rows = await filtered.getAll();
      // console.log(
      //   'Debug - age > 30:',
      //   rows.map((r) => r.name)
      // );
      expect(rows.length).toBe(3); // Charlie, Dave, Eve

      // Test complex condition
      filtered = store.filterSql("(age > 30 AND department = 'Engineering')");
      rows = await filtered.getAll();
      // console.log(
      //   'Debug - age > 30 AND Engineering:',
      //   rows.map((r) => r.name)
      // );
      expect(rows.length).toBe(2); // Charlie, Eve

      // Test OR condition
      filtered = store.filterSql('(isActive = true AND salary > 80000)');
      rows = await filtered.getAll();
      // console.log(
      //   'Debug - active AND salary > 80000:',
      //   rows.map((r) => r.name)
      // );
      expect(rows.length).toBe(1); // Bob

      // Test combined condition
      filtered = store.filterSql(
        "(age > 30 AND department = 'Engineering') OR (isActive = true AND salary > 80000)",
      );
      rows = await filtered.getAll();
      // console.log(
      //   'Debug - Combined condition:',
      //   rows.map((r) => r.name)
      // );
      expect(rows.length).toBe(3); // Charlie, Eve, Bob
    });

    it('should correctly handle SQL projection', async () => {
      // Test projection only
      const result = store.sql('SELECT name, age FROM employees');
      const rows = await result.getAll();

      // console.log('Debug - Projection fields:', Object.keys(rows[0]));
      expect(rows.length).toBe(5);
      expect(Object.keys(rows[0]).sort()).toEqual(['age', 'name']);

      const resolvedTable = await result.resolveTable();
      // Check schema as well
      const columns = resolvedTable.schema.fields;
      // console.log(
      //   'Debug - Schema columns:',
      //   columns.map((c) => c.name)
      // );
      expect(columns.length).toBe(2);
      expect(columns.map((c) => c.name).sort()).toEqual(['age', 'name']);
    });
  });
});
