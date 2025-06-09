import { Table, tableFromArrays } from 'apache-arrow';
import { Aggregations } from '../src/Aggregations';
import { ArrowStore } from '../src/ArrowStore';

// Mock TableSchema type
interface TableSchema {
  tableName: string;
  columns: {
    name: string;
    type: string;
    nullable?: boolean;
  }[];
}

// Test data
const testData = {
  id: [1, 2, 3, 4, 5],
  name: ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
  age: [30, 25, 35, 40, 28],
  department: ['Engineering', 'Product', 'Engineering', 'Sales', 'Product'],
  salary: [100000, 85000, 120000, 95000, 90000],
  isActive: [true, true, false, true, true],
  joinDate: [
    new Date('2020-01-15'),
    new Date('2021-03-10'),
    new Date('2019-11-20'),
    new Date('2022-02-05'),
    new Date('2021-07-22'),
  ],
};

// Create Arrow Table for testing
function createTestTable(): Table {
  return tableFromArrays(testData);
}

// Create a schema for testing
function createTestSchema(): TableSchema {
  return {
    tableName: 'employees',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'string' },
      { name: 'age', type: 'integer' },
      { name: 'department', type: 'string' },
      { name: 'salary', type: 'float' },
      { name: 'isActive', type: 'boolean' },
      { name: 'joinDate', type: 'date' },
    ],
  };
}

describe('ArrowStore', () => {
  let store: ArrowStore<any>;
  let table: Table;
  let schema: TableSchema;

  beforeEach(() => {
    table = createTestTable();
    schema = createTestSchema();
    store = new ArrowStore(table, schema);
  });

  describe('constructor', () => {
    it('creates a new ArrowStore instance', () => {
      expect(store).toBeInstanceOf(ArrowStore);
    });

    it('creates a schema from table if not provided', () => {
      const storeWithoutSchema = new ArrowStore(table);
      expect(storeWithoutSchema.getSchema()).toBeDefined();
      expect(storeWithoutSchema.getSchema().columns.length).toBe(7);
    });
  });

  describe('getSource', () => {
    it('returns the underlying Arrow Table', () => {
      const source = store.getSource();
      expect(source).toBeInstanceOf(Table);
      expect(source.numRows).toBe(5);
    });
  });

  describe('getColumns', () => {
    it('returns the columns from the schema', () => {
      const columns = store.getColumns();
      expect(columns).toEqual(schema.columns);
      expect(columns.length).toBe(7);
    });
  });

  describe('getAll', () => {
    it('returns all records from the table', async () => {
      const results = await store.getAll();
      expect(results.length).toBe(5);
      expect(results[0].name).toBe('Alice');
      expect(results[4].age).toBe(28);
    });
  });

  describe('count', () => {
    it('returns the number of rows in the table', async () => {
      const count = await store.count();
      expect(count).toBe(5);
    });
  });

  describe('sort', () => {
    it('sorts by a single field ascending', async () => {
      const sortedStore = store.sort([{ field: 'age', direction: 'asc' }]);
      const results = await sortedStore.getAll();

      expect(results.length).toBe(5);
      expect(results[0].age).toBe(25);
      expect(results[4].age).toBe(40);
    });

    it('sorts by a single field descending', async () => {
      const sortedStore = store.sort([{ field: 'salary', direction: 'desc' }]);
      const results = await sortedStore.getAll();

      expect(results.length).toBe(5);
      expect(results[0].salary).toBe(120000);
      expect(results[4].salary).toBe(85000);
    });

    it('sorts by multiple fields', async () => {
      const sortedStore = store.sort([
        { field: 'department', direction: 'asc' },
        { field: 'age', direction: 'desc' },
      ]);
      const results = await sortedStore.getAll();

      expect(results.length).toBe(5);

      // First should be Engineering with highest age (Charlie - 35)
      expect(results[0].department).toBe('Engineering');
      expect(results[0].name).toBe('Charlie');

      // Last should be Sales with highest age (Dave - 40)
      expect(results[4].department).toBe('Sales');
      expect(results[4].name).toBe('Dave');
    });

    it('handles dates correctly', async () => {
      const sortedStore = store.sort([{ field: 'joinDate', direction: 'asc' }]);
      const results = await sortedStore.getAll();

      expect(results.length).toBe(5);
      expect(results[0].name).toBe('Charlie'); // 2019-11-20
      expect(results[4].name).toBe('Dave'); // 2022-02-05
    });
  });

  // describe('select', () => {
  //   it('selects a subset of fields', async () => {
  //     const selectedStore = store.select(['name', 'age', 'department']);
  //     const results = await selectedStore.getAll();

  //     expect(results.length).toBe(5);
  //     expect(Object.keys(results[0])).toEqual(['name', 'age', 'department']);
  //     expect(results[0].name).toBe('Alice');
  //     // @ts-ignore
  //     expect(results[0].id).toBeUndefined();
  //   });
  // });

  describe('map', () => {
    it('maps records with a simple transformation', async () => {
      const mappedStore = store.map((item) => ({
        fullName: `${item.name} (${item.department})`,
        ageNextYear: item.age + 1,
        salaryBonus: item.salary * 1.1,
      }));

      const results = await mappedStore.getAll();
      expect(results.length).toBe(5);
      expect(results[0].fullName).toBe('Alice (Engineering)');
      expect(results[0].ageNextYear).toBe(31);
      expect(Math.round(results[0].salaryBonus)).toBe(110000);
    });

    it('handles complex transformations', async () => {
      const mappedStore = store.map((item) => {
        const yearsAtCompany =
          new Date().getFullYear() - new Date(item.joinDate).getFullYear();

        return {
          id: item.id,
          name: item.name,
          status: item.isActive ? 'Active' : 'Inactive',
          experience: yearsAtCompany <= 2 ? 'Junior' : 'Senior',
          payBand: item.salary >= 100000 ? 'A' : 'B',
        };
      });

      const results = await mappedStore.getAll();
      expect(results.length).toBe(5);
      expect(results[0].status).toBe('Active');
      expect(results[2].status).toBe('Inactive');
      expect(results[0].payBand).toBe('A');
      expect(results[1].payBand).toBe('B');
    });
  });

  describe('groupBy', () => {
    it('groups records by a field with count aggregation', async () => {
      const groupedStore = store.groupBy('department', {
        count: Aggregations.count(),
      });

      const results = await groupedStore.getAll();
      expect(results.length).toBe(3);

      const engineering = results.find((r) => r.department === 'Engineering');
      const product = results.find((r) => r.department === 'Product');
      const sales = results.find((r) => r.department === 'Sales');

      expect(engineering?.count).toBe(2);
      expect(product?.count).toBe(2);
      expect(sales?.count).toBe(1);
    });

    it('applies multiple aggregations', async () => {
      const groupedStore = store.groupBy('department', {
        count: Aggregations.count(),
        avgAge: Aggregations.avg('age'),
        totalSalary: Aggregations.sum('salary'),
        minAge: Aggregations.min('age'),
        maxAge: Aggregations.max('age'),
      });

      const results = await groupedStore.getAll();
      expect(results.length).toBe(3);

      const engineering = results.find((r) => r.department === 'Engineering');
      expect(engineering?.count).toBe(2);
      expect(engineering?.avgAge).toBe(32.5);
      expect(engineering?.totalSalary).toBe(220000);
      expect(engineering?.minAge).toBe(30);
      expect(engineering?.maxAge).toBe(35);
    });

    // it('handles null values correctly', async () => {
    //   // Create a table with some null values
    //   const dataWithNulls = { ...testData };
    //   // @ts-ignore
    //   dataWithNulls.age = [30, null, 35, 40, null];

    //   const tableWithNulls = tableFromArrays(dataWithNulls);
    //   const storeWithNulls = new ArrowStore(tableWithNulls, schema);

    //   const groupedStore = storeWithNulls.groupBy('department', {
    //     avgAge: Aggregations.avg('age'),
    //     count: Aggregations.count(),
    //   });

    //   const results = await groupedStore.getAll();

    //   const engineering = results.find((r) => r.department === 'Engineering');
    //   const product = results.find((r) => r.department === 'Product');

    //   expect(engineering?.avgAge).toBe(32.5); // (30 + 35) / 2
    //   expect(product?.avgAge).toBe(40); // One null, one undefined
    // });

    it('applies custom aggregation functions', async () => {
      const customAggregation = {
        // Custom aggregation: median salary
        medianSalary: (values: any[]) => {
          const salaries = values
            .map((record) => record.salary)
            .filter((s) => s !== null && s !== undefined)
            .sort((a, b) => a - b);

          const mid = Math.floor(salaries.length / 2);
          return salaries.length % 2 === 0
            ? (salaries[mid - 1] + salaries[mid]) / 2
            : salaries[mid];
        },
      };

      const groupedStore = store.groupBy('department', customAggregation);
      const results = await groupedStore.getAll();

      const engineering = results.find((r) => r.department === 'Engineering');
      expect(engineering?.medianSalary).toBe(110000); // median of [100000, 120000]
    });
  });

  describe('paginate', () => {
    it('returns a specific page of results', async () => {
      // Create a larger dataset
      const largerData = {
        id: Array.from({ length: 25 }, (_, i) => i + 1),
        name: Array.from({ length: 25 }, (_, i) => `Person ${i + 1}`),
        value: Array.from({ length: 25 }, (_, i) => i * 10),
      };

      const largerTable = tableFromArrays(largerData);
      const largerStore = new ArrowStore(largerTable);

      // Get page 2 with 5 items per page
      const page2 = largerStore.paginate(2, 5);
      const results = await page2.getAll();

      expect(results.length).toBe(5);
      expect(results[0].id).toBe(6);
      expect(results[4].id).toBe(10);
    });

    it('handles edge cases correctly', async () => {
      // Create a dataset
      const data = {
        id: Array.from({ length: 15 }, (_, i) => i + 1),
        name: Array.from({ length: 15 }, (_, i) => `Person ${i + 1}`),
      };

      const table = tableFromArrays(data);
      const testStore = new ArrowStore(table);

      // Last page with less than full items
      const lastPage = testStore.paginate(3, 5);
      const lastPageResults = await lastPage.getAll();
      expect(lastPageResults.length).toBe(5);

      // Page beyond the end
      const beyondEnd = testStore.paginate(4, 5);
      const beyondEndResults = await beyondEnd.getAll();
      expect(beyondEndResults.length).toBe(0);

      // First page
      const firstPage = testStore.paginate(1, 5);
      const firstPageResults = await firstPage.getAll();
      expect(firstPageResults.length).toBe(5);
      expect(firstPageResults[0].id).toBe(1);
    });
  });

  describe('filter methods with serialization', () => {
    it('filters using JSON string representation', async () => {
      const jsonFilters = JSON.stringify([
        {
          type: 'field',
          field: 'department',
          filter: { op: 'eq', value: 'Engineering' },
        },
      ]);

      const filteredStore = store.filterFromJson(jsonFilters);
      const results = await filteredStore.getAll();

      expect(results.length).toBe(2);
      expect(results.map((r) => r.name)).toContain('Alice');
      expect(results.map((r) => r.name)).toContain('Charlie');
    });

    // it('filters using serialized filter conditions', async () => {
    //   const serializedFilters = [
    //     {
    //       type: 'field',
    //       field: 'age',
    //       filter: { op: 'gt', value: 30 },
    //     },
    //   ] as SerializedFilterCondition[];

    //   const filteredStore = store.filterFromSerialized(serializedFilters);
    //   const results = await filteredStore.getAll();

    //   expect(results.length).toBe(2);
    //   expect(results.map((r) => r.name)).toContain('Charlie');
    //   expect(results.map((r) => r.name)).toContain('Dave');
    // });

    it('filters using SQL-like WHERE clause', async () => {
      const sqlFilter = "department = 'Engineering' AND salary > 110000";
      const filteredStore = store.filterSql(sqlFilter);
      const results = await filteredStore.getAll();

      expect(results.length).toBe(1);
      expect(results[0].name).toBe('Charlie');
    });

    it('filters using simple object with field-value pairs', async () => {
      const simpleFilters = {
        department: 'Product',
        isActive: true,
      };

      const filteredStore = store.filterEquals(simpleFilters);
      const results = await filteredStore.getAll();

      expect(results.length).toBe(2);
      expect(results.map((r) => r.name)).toContain('Bob');
      expect(results.map((r) => r.name)).toContain('Eve');
    });

    it('filters using URL query string', async () => {
      const queryString = 'department=Engineering&isActive=true';
      const filteredStore = store.filterFromQueryString(queryString);
      const results = await filteredStore.getAll();

      expect(results.length).toBe(1);
      expect(results[0].name).toBe('Alice');
    });
  });

  describe('flush', () => {
    it('executes all pending operations and returns a new store', async () => {
      // Add multiple operations
      const modifiedStore = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({
          ...item,
          experienceYears: 2023 - new Date(item.joinDate).getFullYear(),
        }));

      // Flush operations
      const flushedStore = await modifiedStore.flush();

      // Check results
      const results = await flushedStore.getAll();
      expect(results.length).toBe(2);
      expect(results[0].name).toBe('Alice');
      expect(results[0].experienceYears).toBeDefined();
    });
  });

  describe('column operations', () => {
    it('calculates sum of a numeric column', async () => {
      const sum = await store.sumColumn('salary');
      expect(sum).toBe(490000);
    });

    it('calculates average of a numeric column', async () => {
      const avg = await store.averageColumn('age');
      expect(avg).toBe(31.6);
    });

    it('finds min and max of a column', async () => {
      const { min, max } = await store.minMaxColumn('age');
      expect(min).toBe(25);
      expect(max).toBe(40);
    });

    it('counts distinct values in a column', async () => {
      const count = await store.countDistinct('department');
      expect(count).toBe(3);
    });
  });

  describe('join', () => {
    it('joins with another ArrowStore on a common key', async () => {
      // Create a second table
      const deptData = {
        department: ['Engineering', 'Product', 'Sales', 'Marketing'],
        budget: [1000000, 500000, 750000, 300000],
        location: ['SF', 'NY', 'LA', 'CHI'],
      };

      const deptTable = tableFromArrays(deptData);
      const deptStore = new ArrowStore(deptTable);

      // Join tables
      const joinedStore = await store.join(deptStore, 'department');
      const results = await joinedStore.getAll();

      expect(results.length).toBe(5);
      expect(results[0].name).toBe('Alice');
      expect(results[0].location).toBe('SF');
      expect(results[0].budget).toBe(1000000);
    });
  });

  describe('SQL query execution', () => {
    it('executes a basic SQL query', async () => {
      const results = await store
        .sql('SELECT name, age FROM employees WHERE age > 30')
        .getAll();

      expect(results.length).toBe(2);
      expect(Object.keys(results[0])).toEqual(['name', 'age']);
      expect(results.map((r) => r.name)).toContain('Charlie');
      expect(results.map((r) => r.name)).toContain('Dave');
    });

    it('executes a SQL query with * projection', async () => {
      const results = await store
        .sql("SELECT * FROM employees WHERE department = 'Product'")
        .getAll();

      expect(results.length).toBe(2);
      expect(results[0].name).toBe('Bob');
      expect(results[1].name).toBe('Eve');
    });
  });

  describe('memory management', () => {
    it('estimates memory usage', async () => {
      const usage = await store.estimateMemoryUsage();

      expect(usage.tableBytes).toBeGreaterThan(0);
      expect(usage.pendingOperations).toBe(0);
      expect(usage.totalEstimatedBytes).toBeGreaterThan(0);
    });

    it('optimizes memory usage', async () => {
      // Add some operations
      const modifiedStore = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({ ...item, modified: true }));

      // Optimize memory
      await modifiedStore.optimizeMemory();

      // Should still return correct results
      const results = await modifiedStore.getAll();
      expect(results.length).toBe(2);
      expect(results[0].modified).toBe(true);
    });
  });

  // describe('error handling', () => {
  //   it('handles errors in filter operations gracefully', async () => {
  //     // Invalid JSON filter
  //     await expect(async () => {
  //       await store.filterFromJson('invalid json').getAll();
  //     }).not.toThrow();

  //     // Invalid SQL filter
  //     await expect(async () => {
  //       await store.filterSql('invalid = sql =').getAll();
  //     }).not.toThrow();
  //   });
  // });
});
