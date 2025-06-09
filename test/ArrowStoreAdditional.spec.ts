import { type Table, tableFromArrays } from 'apache-arrow';
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

// Test data (similar to existing tests for consistency)
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

describe('ArrowStore Additional Tests', () => {
  let store: ArrowStore<any>;
  let table: Table;
  let schema: TableSchema;

  beforeEach(() => {
    table = createTestTable();
    schema = createTestSchema();
    store = new ArrowStore(table, schema);
  });

  describe('Reduce and Fold Operations', () => {
    it('reduces data with accumulator function', async () => {
      // Sum all salaries
      const totalSalary = await store.reduce(
        (acc, item) => acc + item.salary,
        0,
      );
      expect(totalSalary).toBe(490000);

      // Calculate average age
      const ageStats = await store.reduce(
        (acc, item) => {
          acc.sum += item.age;
          acc.count += 1;
          return acc;
        },
        { sum: 0, count: 0 },
      );
      expect(ageStats.sum / ageStats.count).toBe(31.6);
    });

    it('handles fold operation (alias for reduce)', async () => {
      // Build department map with counts
      const deptCounts = await store.fold(
        (acc, item) => {
          const dept = item.department;
          acc[dept] = (acc[dept] || 0) + 1;
          return acc;
        },
        {} as Record<string, number>,
      );

      expect(deptCounts['Engineering']).toBe(2);
      expect(deptCounts['Product']).toBe(2);
      expect(deptCounts['Sales']).toBe(1);
    });

    it('handles reduceRight operation (reverse order)', async () => {
      // Build array of names in reverse order
      const namesReversed = await store.reduceRight((acc, item) => {
        acc.push(item.name);
        return acc;
      }, [] as string[]);

      expect(namesReversed).toEqual(['Eve', 'Dave', 'Charlie', 'Bob', 'Alice']);
    });

    it('handles foldRight operation (alias for reduceRight)', async () => {
      // Build concatenated string of names in reverse order
      const nameString = await store.foldRight((acc, item, index) => {
        return index === 4 ? item.name : acc + ', ' + item.name;
      }, '');

      expect(nameString).toBe('Eve, Dave, Charlie, Bob, Alice');
    });

    it('handles reduce on empty table', async () => {
      // Create empty table with same schema
      const emptyTable = tableFromArrays({
        id: [],
        name: [],
        age: [],
        department: [],
        salary: [],
        isActive: [],
        joinDate: [],
      });

      const emptyStore = new ArrowStore(emptyTable, schema);
      const result = await emptyStore.reduce((acc, _) => acc + 1, 0);
      expect(result).toBe(0);
    });
  });

  describe('Set and Map Conversions', () => {
    it('converts table to Set with identity function', async () => {
      // Use a simpler dataset for this test
      const simpleData = {
        id: [1, 2, 3, 3, 4], // Note the duplicate 3
        value: ['a', 'b', 'c', 'c', 'd'],
      };
      const simpleTable = tableFromArrays(simpleData);
      const simpleStore = new ArrowStore(simpleTable);

      // Convert to Set (should deduplicate rows)
      const resultSet = await simpleStore.toSet();

      // Since Set contains complex objects, checking size is more reliable
      expect(resultSet.size).toBe(4); // Should deduplicate the repeated row
    });

    it('converts table to Set with key function', async () => {
      // Convert to Set of departments (should deduplicate)
      const deptSet = await store.toSet((item) => item.department);

      expect(deptSet.size).toBe(3);
      expect(deptSet.has('Engineering')).toBe(true);
      expect(deptSet.has('Product')).toBe(true);
      expect(deptSet.has('Sales')).toBe(true);
    });

    it('converts table to Map with key and value functions', async () => {
      // Map of id -> name
      const idToNameMap = await store.toMap(
        (item) => item.id,
        (item) => item.name,
      );

      expect(idToNameMap.size).toBe(5);
      expect(idToNameMap.get(1)).toBe('Alice');
      expect(idToNameMap.get(3)).toBe('Charlie');
      expect(idToNameMap.get(5)).toBe('Eve');
    });

    it('converts table to Map with default value function', async () => {
      // Map of name -> full row
      const nameToRowMap = await store.toMap((item) => item.name);

      expect(nameToRowMap.size).toBe(5);
      expect(nameToRowMap.get('Alice').id).toBe(1);
      expect(nameToRowMap.get('Alice').department).toBe('Engineering');
      expect(nameToRowMap.get('Charlie').age).toBe(35);
    });

    it('handles toMap and toSet on empty table', async () => {
      // Create empty table with same schema
      const emptyTable = tableFromArrays({
        id: [],
        name: [],
        age: [],
        department: [],
        salary: [],
        isActive: [],
        joinDate: [],
      });

      const emptyStore = new ArrowStore(emptyTable, schema);

      const emptySet = await emptyStore.toSet();
      expect(emptySet.size).toBe(0);

      const emptyMap = await emptyStore.toMap((item) => item.id);
      expect(emptyMap.size).toBe(0);
    });
  });

  describe('Memory Management', () => {
    it('returns meaningful memory estimates', async () => {
      const memUsage = await store.estimateMemoryUsage();

      expect(memUsage.tableBytes).toBeGreaterThan(0);
      expect(memUsage.pendingOperations).toBe(0);
      expect(memUsage.totalEstimatedBytes).toBeGreaterThan(0);
    });

    it('includes pending operations in memory estimate', async () => {
      // Add some pending operations
      const pendingStore = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({ ...item, bonus: item.salary * 0.1 }));

      const memUsage = await pendingStore.estimateMemoryUsage();

      expect(memUsage.pendingOperations).toBe(2);
      expect(memUsage.totalEstimatedBytes).toBeGreaterThan(memUsage.tableBytes);
    });

    it('optimizes memory by flushing pending operations', async () => {
      // Add some pending operations
      const pendingStore = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({ ...item, bonus: item.salary * 0.1 }));

      // Before optimization
      const beforeMem = await pendingStore.estimateMemoryUsage();
      expect(beforeMem.pendingOperations).toBe(2);

      // Optimize memory
      await pendingStore.optimizeMemory();

      // After optimization
      const afterMem = await pendingStore.estimateMemoryUsage();
      expect(afterMem.pendingOperations).toBe(0);

      // Verify data is still correct
      const results = await pendingStore.getAll();
      expect(results.length).toBe(2);
      expect(results[0].department).toBe('Engineering');
      expect(results[0].bonus).toBe(10000);
    });
  });

  describe('Batch Processing', () => {
    it('processes large datasets in batches', async () => {
      // Create a larger dataset to test batch processing
      const largeDataSize = 10000;
      const largeData = {
        id: Array.from({ length: largeDataSize }, (_, i) => i),
        value: Array.from({ length: largeDataSize }, (_, i) => i * 10),
      };

      const largeTable = tableFromArrays(largeData);
      const largeStore = new ArrowStore(largeTable);

      // Test a complex operation that should use batch processing internally
      const result = await largeStore
        .map((item) => ({
          id: item.id,
          value: item.value,
          squared: item.value * item.value,
        }))
        .filter([{ field: 'squared', filter: { op: 'gt', value: 100000 } }])
        .getAll();

      // Check a sample of results
      expect(result.length).toBe(largeDataSize - 32); // Values 0-31 have squares <= 10000
      expect(result[0].id).toBe(32);
      expect(result[0].squared).toBe(102400);
    });

    it('resolves all pending operations correctly', async () => {
      // Add multiple operations
      const complexStore = store
        .filter([{ field: 'age', filter: { op: 'gt', value: 25 } }])
        .map((item) => ({ ...item, ageGroup: Math.floor(item.age / 10) * 10 }))
        .filter([{ field: 'ageGroup', filter: { op: 'eq', value: 30 } }])
        .map((item) => ({ ...item, label: `${item.name} (${item.age})` }));

      // Resolve all operations and check results
      const results = await complexStore.getAll();

      expect(results.length).toBe(2);
      expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie']);
      expect(results[0].ageGroup).toBe(30);
      expect(results[0].label).toBe(
        results[0].name + ' (' + results[0].age + ')',
      );
    });
  });

  describe('Column-specific Operations', () => {
    it('gets column as Vector', async () => {
      const nameCol = store.getColumn('name');
      expect(nameCol).not.toBeNull();
      expect(nameCol?.length).toBe(5);
      expect(nameCol?.get(0)).toBe('Alice');
      expect(nameCol?.get(2)).toBe('Charlie');
    });

    it('gets column by index', async () => {
      const ageCol = store.getColumnAt(2); // age is at index 2
      expect(ageCol).not.toBeNull();
      expect(ageCol?.length).toBe(5);
      expect(ageCol?.get(0)).toBe(30);
    });

    it('handles invalid column requests', async () => {
      const invalidCol1 = store.getColumn('nonExistent' as any);
      expect(invalidCol1).toBeNull();

      const invalidCol2 = store.getColumnAt(99);
      expect(invalidCol2).toBeNull();
    });

    it('calculates sum of column efficiently', async () => {
      const salarySum = await store.sumColumn('salary');
      expect(salarySum).toBe(490000);
    });

    it('calculates average of column', async () => {
      const ageAvg = await store.averageColumn('age');
      expect(ageAvg).toBe(31.6);
    });

    it('finds min and max of column', async () => {
      const ageBounds = await store.minMaxColumn('age');
      expect(ageBounds.min).toBe(25);
      expect(ageBounds.max).toBe(40);
    });

    it('counts distinct values in column', async () => {
      const distinctDepts = await store.countDistinct('department');
      expect(distinctDepts).toBe(3);
    });
  });

  describe('Error Handling', () => {
    it('handles errors when field does not exist in filter operations', async () => {
      try {
        await store
          .filter([
            {
              field: 'nonExistentField' as any,
              filter: { op: 'eq', value: 'value' },
            },
          ])
          .getAll();
        fail('Should have thrown an error');
      } catch (error) {
        expect(error instanceof Error).toBe(true);
        expect((error as Error).message).toContain('Filter error: Field');
        expect((error as Error).message).toContain('does not exist');
      }
    });

    it('catches errors in map operations', async () => {
      try {
        await store
          .map((item) => {
            // Deliberately access a property that doesn't exist
            return { computed: (item as any).nonExistentProp.someField };
          })
          .getAll();
        fail('Should have thrown an error');
      } catch (error) {
        expect(error instanceof Error).toBe(true);
      }
    });

    it('catches errors in reduce operations', async () => {
      try {
        await store.reduce((acc, item) => {
          // Deliberately cause an error
          if (item.name === 'Charlie') {
            throw new Error('Test error');
          }
          return acc;
        }, 0);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error instanceof Error).toBe(true);
        expect((error as Error).message).toContain('Error in reduce operation');
      }
    });

    it('validates SQL filter syntax', async () => {
      try {
        await store
          .filterSql('department = Engineering') // Missing quotes
          .getAll();
        fail('Should have thrown an error');
      } catch (error) {
        expect(error instanceof Error).toBe(true);
        expect((error as Error).message).toContain('Invalid SQL filter');
      }
    });

    it('validates SQL query format', async () => {
      try {
        await store.sql('UPDATE employees SET age = 30').getAll();
        fail('Should have thrown an error');
      } catch (error) {
        expect(error instanceof Error).toBe(true);
        expect((error as Error).message).toContain(
          'Only SELECT statements are supported',
        );
      }
    });
  });

  describe('Advanced Operations', () => {
    it('selects subset of columns by index', async () => {
      const selected = store.selectAt([1, 2]); // should select name and age
      const results = await selected.getAll();

      expect(results.length).toBe(5);
      expect(Object.keys(results[0]).sort()).toEqual(['age', 'name']);
    });

    it('combines with another ArrowStore', async () => {
      // Create another store with different data
      const extraData = {
        id: [1, 2, 3, 4, 5],
        bonus: [5000, 4000, 6000, 3000, 4500],
        team: ['Alpha', 'Beta', 'Alpha', 'Gamma', 'Beta'],
      };

      const extraTable = tableFromArrays(extraData);
      const extraStore = new ArrowStore(extraTable);

      // Combine the stores
      const combined = await store.combine(extraStore);
      const results = await combined.getAll();

      expect(results.length).toBe(5);
      expect(Object.keys(results[0]).includes('bonus')).toBe(true);
      expect(Object.keys(results[0]).includes('team')).toBe(true);
      expect(results[0].name).toBe('Alice');
      expect(results[0].bonus).toBe(5000);
      expect(results[0].team).toBe('Alpha');
    });

    it('performs slice operation', async () => {
      const sliced = store.slice(1, 4);
      const results = await sliced.getAll();

      expect(results.length).toBe(3);
      expect(results[0].name).toBe('Bob');
      expect(results[2].name).toBe('Dave');
    });

    it('converts to record batch', async () => {
      const batch = await store.toRecordBatch();

      expect(batch).toBeDefined();
      expect(batch.numRows).toBe(5);
      expect(batch.numCols).toBe(7);
    });
  });

  describe('Memory Usage Estimation', () => {
    it('provides accurate memory estimates for simple data', async () => {
      // Create a table with known data types
      const simpleData = {
        id: Int32Array.from({ length: 1000 }, (_, i) => i), // 4 bytes per value
        name: Array.from({ length: 1000 }, (_, i) => `Name${i}`), // Variable string size
        active: Array.from({ length: 1000 }, () => Math.random() > 0.5), // Boolean
        value: Float64Array.from({ length: 1000 }, () => Math.random() * 1000), // 8 bytes per value
      };

      const simpleTable = tableFromArrays(simpleData);
      const simpleStore = new ArrowStore(simpleTable);

      // Get memory estimates
      const memUsage = await simpleStore.estimateMemoryUsage();

      // Verify structure and values
      expect(memUsage.tableBytes).toBeGreaterThan(0);
      expect(memUsage.columnarDataBytes).toBeGreaterThan(0);
      expect(memUsage.metadataBytes).toBeGreaterThan(0);
      expect(memUsage.pendingOperations).toBe(0);
      expect(memUsage.totalEstimatedBytes).toBeGreaterThan(0);

      // Expected minimum sizes based on data types
      // Int32Array: 1000 * 4 = 4,000 bytes
      // Float64Array: 1000 * 8 = 8,000 bytes
      // Boolean: ~125 bytes (bit-packed)
      // Strings: variable, but should be significant
      expect(memUsage.columnarDataBytes).toBeGreaterThan(12000);

      // Human readable format check
      expect(typeof memUsage.humanReadable.total).toBe('string');
      expect(memUsage.humanReadable.total).toContain('KB');
    });

    it('estimates memory usage with pending operations', async () => {
      // Create store with pending operations
      const storeWithOperations = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({ ...item, bonus: item.salary * 0.1 }))
        .sort([{ field: 'bonus', direction: 'desc' }]);

      // Get memory estimates
      const memUsage = await storeWithOperations.estimateMemoryUsage();

      // Check operation count
      expect(memUsage.pendingOperations).toBe(3);
      expect(memUsage.estimatedOperationOverhead).toBeGreaterThan(0);

      // Verify total includes operations overhead
      expect(memUsage.totalEstimatedBytes).toBe(
        memUsage.tableBytes + memUsage.estimatedOperationOverhead,
      );

      // Human readable operation size should be present
      expect(memUsage.humanReadable.operations).toBeDefined();
    });

    it('handles large datasets with reasonable performance', async () => {
      // Create a larger dataset to test estimation performance
      const size = 100000;
      const largeData = {
        id: Int32Array.from({ length: size }, (_, i) => i),
        value: Float64Array.from({ length: size }, () => Math.random() * 1000),
        category: Array.from(
          { length: size },
          () => ['A', 'B', 'C', 'D', 'E'][Math.floor(Math.random() * 5)],
        ),
        active: Array.from({ length: size }, () => Math.random() > 0.3),
      };

      const largeTable = tableFromArrays(largeData);
      const largeStore = new ArrowStore(largeTable);

      // Measure execution time
      const startTime = Date.now();
      const memUsage = await largeStore.estimateMemoryUsage();
      const endTime = Date.now();

      // Calculate execution time
      const executionTime = endTime - startTime;

      // Verify we got reasonable estimates
      expect(memUsage.tableBytes).toBeGreaterThan(0);
      expect(memUsage.columnarDataBytes).toBeGreaterThan(0);

      // Check execution time is reasonable for this size (should be fast)
      expect(executionTime).toBeLessThan(1000); // Should complete in under 1 second

      // Expected minimum bytes based on known data
      // Int32Array: 100000 * 4 = 400,000 bytes
      // Float64Array: 100000 * 8 = 800,000 bytes
      // Plus strings and booleans
      const expectedMinBytes = 1200000;
      expect(memUsage.columnarDataBytes).toBeGreaterThan(expectedMinBytes);

      // Human readable format should show MB for this size
      expect(memUsage.humanReadable.columnarData).toContain('MB');
    });

    it('reports accurate estimates after flushing operations', async () => {
      // Create store with operations
      const storeWithOperations = store
        .filter([
          { field: 'department', filter: { op: 'eq', value: 'Engineering' } },
        ])
        .map((item) => ({ ...item, bonus: item.salary * 0.1 }));

      // Get memory estimates before flush
      const beforeFlush = await storeWithOperations.estimateMemoryUsage();
      expect(beforeFlush.pendingOperations).toBe(2);

      // Flush operations
      const flushedStore = await storeWithOperations.flush();

      // Get memory estimates after flush
      const afterFlush = await flushedStore.estimateMemoryUsage();
      expect(afterFlush.pendingOperations).toBe(0);

      // After flush, the table bytes might be different (fewer rows due to filter)
      // but operation overhead should be gone
      expect(afterFlush.estimatedOperationOverhead).toBe(0);

      // The flushed store should have the Engineering department data only
      const results = await flushedStore.getAll();
      expect(results.length).toBe(2); // 2 Engineering department entries
      expect(results.every((r) => r.department === 'Engineering')).toBe(true);
    });
  });
});
