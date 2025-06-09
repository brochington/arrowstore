import * as arrow from 'apache-arrow';
import { ArrowStore } from '../src/ArrowStore';
import * as filters from '../src/filter-helpers';

// Helper function to create a test table with realistic test data
function createTestTable() {
  const LENGTH = 100; // Number of records to generate

  // Sample data for generating test records
  const names = [
    'Alice',
    'Bob',
    'Charlie',
    'David',
    'Eve',
    'Frank',
    'Grace',
    'Hannah',
    'Ian',
    'Julia',
  ];
  const departments = ['Engineering', 'Product', 'Sales', 'Marketing', 'HR'];
  const emailDomains = ['company.com', 'gmail.com', 'yahoo.com', 'outlook.com'];
  // const programmingLanguages = [
  //   'js',
  //   'python',
  //   'java',
  //   'ruby',
  //   'go',
  //   'rust',
  //   'c#',
  //   'php',
  //   'swift',
  //   'kotlin',
  // ];
  // const frameworks = [
  //   'react',
  //   'angular',
  //   'vue',
  //   'django',
  //   'flask',
  //   'spring',
  //   'rails',
  //   'gin',
  //   'express',
  //   'laravel',
  // ];

  // Generate typed arrays and arrays for better performance
  const ids = Int32Array.from({ length: LENGTH }, (_, i) => i + 1);

  const ages = Int32Array.from(
    { length: LENGTH },
    () => Math.floor(Math.random() * 30) + 25, // Ages between 25-54
  );

  const salaries = Float32Array.from(
    { length: LENGTH },
    () => Math.floor(Math.random() * 100000) + 50000, // Salaries between 50k-150k
  );

  // Generate regular arrays for the other fields
  const namesArray = Array.from(
    { length: LENGTH },
    () => names[Math.floor(Math.random() * names.length)],
  );

  const departmentsArray = Array.from(
    { length: LENGTH },
    () => departments[Math.floor(Math.random() * departments.length)],
  );

  const isActiveArray = Array.from(
    { length: LENGTH },
    () => Math.random() > 0.3, // 70% chance of being active
  );

  // const tagsArray = Array.from({ length: LENGTH }, () => {
  //   const lang =
  //     programmingLanguages[
  //       Math.floor(Math.random() * programmingLanguages.length)
  //     ];
  //   const framework = frameworks[Math.floor(Math.random() * frameworks.length)];
  //   return [lang, framework];
  // });

  const emailArray = Array.from({ length: LENGTH }, (_, i) => {
    const name = namesArray[i].toLowerCase();
    const domain =
      emailDomains[Math.floor(Math.random() * emailDomains.length)];
    return `${name}${i + 1}@${domain}`;
  });

  const hireDate = Array.from(
    { length: LENGTH },
    () =>
      new Date(
        Date.now() - Math.floor(Math.random() * 1000 * 60 * 60 * 24 * 365 * 5),
      ), // Random date within last 5 years
  );

  // Combine all the generated data
  const data = {
    id: ids,
    name: namesArray,
    age: ages,
    department: departmentsArray,
    salary: salaries,
    isActive: isActiveArray,
    // tags: tagsArray,
    email: emailArray,
    hireDate: hireDate,
  };

  return arrow.tableFromArrays(data);
}

// Test suite for ArrowStore filtering functionality
describe('ArrowStore Filtering', () => {
  let table: arrow.Table;
  let store: ArrowStore<any>;
  let simpleStore: ArrowStore<any>;

  // Set up before each test
  beforeEach(() => {
    try {
      table = createTestTable();
      store = new ArrowStore(table);
      simpleStore = new ArrowStore(createSimpleTestTable());
    } catch (error) {
      console.error('Error creating test table:', error);
    }
  });

  // Helper function to create a simple test table with specific data for predictable tests
  function createSimpleTestTable() {
    const data = {
      id: Int32Array.from([1, 2, 3, 4, 5]),
      name: ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
      age: Int32Array.from([25, 30, 35, 40, 45]),
      department: [
        'Engineering',
        'Product',
        'Engineering',
        'Sales',
        'Engineering',
      ],
      salary: Float32Array.from([100000, 110000, 120000, 90000, 130000]),
      isActive: [true, true, false, true, false],
      email: [
        'alice@company.com',
        'bob@gmail.com',
        'charlie@company.com',
        'david@yahoo.com',
        'eve@company.com',
      ],
    };

    return arrow.tableFromArrays(data);
  }

  describe('Basic Filtering', () => {
    it('should filter by equality condition', async () => {
      // Use simple test table for predictable results

      const result = await simpleStore
        .filter([filters.field('age', filters.eq(30))])
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Bob');
    });

    it('should filter by inequality condition', async () => {
      const result = await simpleStore
        .filter([filters.field('age', filters.neq(30))])
        .getAll();

      expect(result).toHaveLength(4);
      expect(result.map((r) => r.name)).not.toContain('Bob');
    });

    it('should filter by greater than condition', async () => {
      const result = await simpleStore
        .filter([filters.field('age', filters.gt(35))])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['David', 'Eve']),
      );
    });

    it('should filter by greater than or equal condition', async () => {
      const result = await simpleStore
        .filter([filters.field('age', filters.gte(35))])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Charlie', 'David', 'Eve']),
      );
    });

    it('should filter by less than condition', async () => {
      const result = await simpleStore
        .filter([filters.field('age', filters.lt(30))])
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });

    it('should filter by less than or equal condition', async () => {
      const result = await simpleStore
        .filter([filters.field('age', filters.lte(30))])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob']),
      );
    });

    it('should filter by boolean condition', async () => {
      const result = await simpleStore
        .filter([filters.field('isActive', filters.eq(true))])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob', 'David']),
      );
    });
  });

  describe('String Operations', () => {
    it('should filter with contains operator', async () => {
      const result = await simpleStore
        .filter([filters.field('name', filters.contains('a'))])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Charlie', 'David']),
      );
    });

    it('should filter with startsWith operator', async () => {
      const result = await simpleStore
        .filter([filters.field('name', filters.startsWith('D'))])
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('David');
    });

    it('should filter with endsWith operator', async () => {
      const result = await simpleStore
        .filter([filters.field('name', filters.endsWith('e'))])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Charlie', 'Eve']),
      );
    });

    it('should filter emails with endsWith operator', async () => {
      const result = await simpleStore
        .filter([filters.field('email', filters.endsWith('@company.com'))])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Charlie', 'Eve']),
      );
    });
  });

  describe('Array Operations', () => {
    it('should filter with inArray operator', async () => {
      const result = await simpleStore
        .filter([
          filters.field(
            'department',
            filters.inArray(['Engineering', 'Product']),
          ),
        ])
        .getAll();

      expect(result).toHaveLength(4);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob', 'Charlie', 'Eve']),
      );
      expect(result.map((r) => r.name)).not.toContain('David');
    });
  });

  describe('Multiple Conditions', () => {
    it('should apply multiple filters as implicit AND', async () => {
      const result = await simpleStore
        .filter([
          filters.field('department', filters.eq('Engineering')),
          filters.field('isActive', filters.eq(true)),
        ])
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('Complex Filters - AND', () => {
    it('should apply AND filter with multiple conditions', async () => {
      const result = await simpleStore
        .filter([
          filters.and([
            filters.field('age', filters.gt(25)),
            filters.field('age', filters.lt(40)),
          ]),
        ])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Bob', 'Charlie']),
      );
    });

    it('should apply AND filter with different fields', async () => {
      const result = await simpleStore
        .filter([
          filters.and([
            filters.field('department', filters.eq('Engineering')),
            filters.field('salary', filters.gt(110000)),
          ]),
        ])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Charlie', 'Eve']),
      );
    });
  });

  describe('Complex Filters - OR', () => {
    it('should apply OR filter with multiple conditions', async () => {
      const result = await simpleStore
        .filter([
          filters.or([
            filters.field('department', filters.eq('Sales')),
            filters.field('salary', filters.gt(120000)),
          ]),
        ])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['David', 'Eve']),
      );
    });
  });

  describe('Complex Filters - NOT', () => {
    it('should apply NOT filter', async () => {
      const result = await simpleStore
        .filter([
          filters.not(filters.field('department', filters.eq('Engineering'))),
        ])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Bob', 'David']),
      );
    });
  });

  describe('Nested Complex Filters', () => {
    it('should apply nested AND and OR conditions', async () => {
      const result = await simpleStore
        .filter([
          filters.field('isActive', filters.eq(true)),
          filters.or([
            filters.field('department', filters.eq('Engineering')),
            filters.and([
              filters.field('department', filters.eq('Product')),
              filters.field('age', filters.gt(25)),
            ]),
          ]),
        ])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob']),
      );
    });

    it('should apply NOT with nested conditions', async () => {
      const result = await simpleStore
        .filter([
          filters.not(
            filters.or([
              filters.field('age', filters.lt(30)),
              filters.field('age', filters.gt(40)),
            ]),
          ),
        ])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Bob', 'Charlie', 'David']),
      );
    });
  });

  describe('Alternative Filter Methods', () => {
    it('filterFromJson should correctly apply filters', async () => {
      const jsonFilters = JSON.stringify([
        {
          type: 'field',
          field: 'department',
          filter: { op: 'eq', value: 'Engineering' },
        },
        { type: 'field', field: 'isActive', filter: { op: 'eq', value: true } },
      ]);

      const result = await simpleStore.filterFromJson(jsonFilters).getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });

    // it('filterFromSerialized should correctly apply filters', async () => {
    //   const serializedFilters = [
    //     {
    //       type: 'field',
    //       field: 'department',
    //       filter: { op: 'eq', value: 'Engineering' },
    //     },
    //     { type: 'field', field: 'isActive', filter: { op: 'eq', value: true } },
    //   ];

    //   const result = await store
    //     .filterFromSerialized(serializedFilters)
    //     .getAll();

    //   expect(result).toHaveLength(1);
    //   expect(result[0].name).toBe('Alice');
    // });

    it('filterEquals should correctly apply filters', async () => {
      const result = await simpleStore
        .filterEquals({
          department: 'Engineering',
          isActive: true,
        })
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });

    it('filterFromQueryString should correctly apply filters', async () => {
      const queryString = 'department=Engineering&isActive=true';

      const result = await simpleStore
        .filterFromQueryString(queryString)
        .getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });

    it('filterSql should correctly apply filters', async () => {
      const sqlFilter = "department = 'Engineering' AND isActive = true";

      const result = await simpleStore.filterSql(sqlFilter).getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('Dynamic Filter Building', () => {
    it('should build filters dynamically based on options', async () => {
      // Simulate a filter builder function similar to the documentation example
      function buildUserFilter(options: {
        isActive?: boolean;
        minAge?: number;
        maxAge?: number;
        departments?: string[];
        searchTerm?: string;
      }) {
        const filterConditions: filters.FilterCondition<any>[] = [];

        // Add filters based on options
        if (options.isActive !== undefined) {
          filterConditions.push(
            filters.field('isActive', filters.eq(options.isActive)),
          );
        }

        // Age range filter
        const ageFilters: filters.FilterCondition<any>[] = [];
        if (options.minAge !== undefined) {
          ageFilters.push(filters.field('age', filters.gte(options.minAge)));
        }
        if (options.maxAge !== undefined) {
          ageFilters.push(filters.field('age', filters.lte(options.maxAge)));
        }
        if (ageFilters.length > 0) {
          filterConditions.push(filters.and(ageFilters));
        }

        // Department filter
        if (options.departments && options.departments.length > 0) {
          filterConditions.push(
            filters.field('department', filters.inArray(options.departments)),
          );
        }

        // Search filter
        if (options.searchTerm) {
          filterConditions.push(
            filters.or([
              filters.field('name', filters.contains(options.searchTerm)),
              filters.field('email', filters.contains(options.searchTerm)),
            ]),
          );
        }

        return filterConditions;
      }

      // Test with various option combinations
      const filterOptions = {
        isActive: true,
        minAge: 25,
        maxAge: 35,
        departments: ['Engineering', 'Product'],
        searchTerm: 'a',
      };

      const dynamicFilters = buildUserFilter(filterOptions);
      const result = await simpleStore.filter(dynamicFilters).getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob']),
      );
    });
  });

  describe('Filter Serialization and Deserialization', () => {
    it('should serialize and deserialize filters correctly', async () => {
      const originalFilters = [
        filters.field('department', filters.eq('Engineering')),
        filters.field('isActive', filters.eq(true)),
      ];

      // Serialize to JSON
      const jsonString = simpleStore.serializeFilters(originalFilters);

      // Use the JSON string with filterFromJson
      const result = await simpleStore.filterFromJson(jsonString).getAll();

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('Filter Edge Cases', () => {
    it('should handle empty filter array', async () => {
      const result = await simpleStore.filter([]).getAll();
      expect(result.length).toBeGreaterThan(0); // Should return all rows
    });

    it('should handle non-existent field', async () => {
      try {
        await simpleStore
          .filter([
            filters.field('nonExistentField' as any, filters.eq('something')),
          ])
          .getAll();
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
      }
    });

    it('should handle filtering on null values', async () => {
      // Create a table with null values
      const dataWithNulls = {
        id: Int32Array.from({ length: 5 }, (_, i) => i + 1),
        name: ['Alice', null, 'Charlie', 'David', null],
      };

      const tableWithNulls = arrow.tableFromArrays(dataWithNulls);
      const storeWithNulls = new ArrowStore(tableWithNulls);

      const result = await storeWithNulls
        .filter([filters.field('name', filters.eq(null))])
        .getAll();

      expect(result).toHaveLength(2);
      expect(result.map((r) => r.id)).toEqual(expect.arrayContaining([2, 5]));
    });

    it('should handle date filtering', async () => {
      // Generate dates for testing
      const now = new Date();
      const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      const lastWeek = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

      // Create a table with dates
      const dataWithDates = {
        id: Int32Array.from({ length: 5 }, (_, i) => i + 1),
        name: ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        createdAt: [now, yesterday, lastWeek, lastWeek, now],
      };

      const tableWithDates = arrow.tableFromArrays(dataWithDates);
      const storeWithDates = new ArrowStore(tableWithDates);

      const result = await storeWithDates
        .filter([filters.field('createdAt', filters.gt(lastWeek))])
        .getAll();

      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name)).toEqual(
        expect.arrayContaining(['Alice', 'Bob', 'Eve']),
      );
    });
  });

  describe('Performance Optimizations', () => {
    it('should handle large datasets efficiently', async () => {
      // Create a larger dataset to test performance
      const LENGTH = 10000;

      // Generate data using typed arrays for better performance
      const largeData = {
        id: Int32Array.from({ length: LENGTH }, (_, i) => i),
        value: Float32Array.from(
          { length: LENGTH },
          () => Math.random() * 1000,
        ),
        category: Array.from({ length: LENGTH }, (_, i) =>
          i % 5 === 0 ? 'A' : i % 3 === 0 ? 'B' : 'C',
        ),
        timestamp: Array.from(
          { length: LENGTH },
          (_, i) => new Date(Date.now() - i * 60000), // One minute apart
        ),
      };

      const largeTable = arrow.tableFromArrays(largeData);
      const largeStore = new ArrowStore(largeTable);

      // Measure execution time
      const startTime = Date.now();

      const result = await largeStore
        .filter([
          filters.field('category', filters.eq('A')),
          filters.field('value', filters.gt(500)),
        ])
        .getAll();

      const endTime = Date.now();
      const executionTime = endTime - startTime;

      // Verify results are correct
      expect(result.length).toBeGreaterThan(0);
      expect(result.every((r) => r.category === 'A' && r.value > 500)).toBe(
        true,
      );

      // This is a soft assertion - mainly to document the performance
      // In a real test environment, you might want to compare against a baseline
      console.log(`Large dataset filter executed in ${executionTime}ms`);
      expect(executionTime).toBeLessThan(1000); // Should execute in under 1 second with the optimized data structure
    });
  });
});
