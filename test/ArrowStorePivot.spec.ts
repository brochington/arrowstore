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

// Test data for pivot and unpivot operations
const salesData = {
  product: ['Widget', 'Gadget', 'Widget', 'Gadget', 'Widget', 'Gadget'],
  region: ['North', 'North', 'South', 'South', 'East', 'East'],
  year: [2021, 2021, 2021, 2021, 2022, 2022],
  quarter: ['Q1', 'Q1', 'Q2', 'Q2', 'Q1', 'Q1'],
  sales: [100, 150, 120, 180, 90, 200],
  units: [10, 15, 12, 18, 9, 20],
};

// Test data for unpivot operations
const revenueByQuarter = {
  product: ['Widget', 'Gadget', 'Accessory'],
  Q1_2021: [100, 150, 80],
  Q2_2021: [120, 180, 90],
  Q3_2021: [110, 160, 85],
  Q4_2021: [130, 190, 95],
  Q1_2022: [90, 200, 100],
  Q2_2022: [95, 210, 105],
};

// Create Arrow Tables for testing
function createSalesTable(): Table {
  return tableFromArrays(salesData);
}

function createRevenueByQuarterTable(): Table {
  return tableFromArrays(revenueByQuarter);
}

// Create schemas for testing
function createSalesSchema(): TableSchema {
  return {
    tableName: 'sales',
    columns: [
      { name: 'product', type: 'string' },
      { name: 'region', type: 'string' },
      { name: 'year', type: 'integer' },
      { name: 'quarter', type: 'string' },
      { name: 'sales', type: 'float' },
      { name: 'units', type: 'integer' },
    ],
  };
}

function createRevenueByQuarterSchema(): TableSchema {
  return {
    tableName: 'revenue_by_quarter',
    columns: [
      { name: 'product', type: 'string' },
      { name: 'Q1_2021', type: 'float' },
      { name: 'Q2_2021', type: 'float' },
      { name: 'Q3_2021', type: 'float' },
      { name: 'Q4_2021', type: 'float' },
      { name: 'Q1_2022', type: 'float' },
      { name: 'Q2_2022', type: 'float' },
    ],
  };
}

describe('ArrowStore - Pivot Operations', () => {
  let store: ArrowStore<any>;
  let table: Table;
  let schema: TableSchema;

  beforeEach(() => {
    table = createSalesTable();
    schema = createSalesSchema();
    store = new ArrowStore(table, schema);
  });

  describe('pivot', () => {
    it('pivots data with a single pivot column and sum aggregation', async () => {
      const pivotedStore = store.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
          },
        ],
        groupBy: ['product', 'year'],
      });

      const results = await pivotedStore.getAll();

      expect(results.length).toBe(4); // 2 products × 2 years

      // Widget in 2021
      expect(results[0].product).toBe('Widget');
      expect(results[0].year).toBe(2021);
      expect(results[0].North).toBe(100);
      expect(results[0].South).toBe(120);

      // Gadget in 2021
      expect(results[1].product).toBe('Gadget');
      expect(results[1].year).toBe(2021);
      expect(results[1].North).toBe(150);
      expect(results[1].South).toBe(180);

      // Widget in 2022
      expect(results[2].product).toBe('Widget');
      expect(results[2].year).toBe(2022);
      expect(results[2].East).toBe(90);

      // Gadget in 2022
      expect(results[3].product).toBe('Gadget');
      expect(results[3].year).toBe(2022);
      expect(results[3].East).toBe(200);
    });

    it('pivots data with multiple pivot columns and count aggregation', async () => {
      const pivotedStore = store.pivot({
        on: ['region', 'quarter'],
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.length,
          },
        ],
        groupBy: ['product', 'year'],
      });

      const results = await pivotedStore.getAll();

      expect(results.length).toBe(4); // 2 products × 2 years

      const expectedResults = [
        {
          product: 'Widget',
          year: 2021,
          North_Q1: 1,
          North_Q2: 0,
          South_Q1: 0,
          South_Q2: 1,
          East_Q1: 0,
          East_Q2: 0,
        },
        {
          product: 'Gadget',
          year: 2021,
          North_Q1: 1,
          North_Q2: 0,
          South_Q1: 0,
          South_Q2: 1,
          East_Q1: 0,
          East_Q2: 0,
        },
        {
          product: 'Widget',
          year: 2022,
          North_Q1: 0,
          North_Q2: 0,
          South_Q1: 0,
          South_Q2: 0,
          East_Q1: 1,
          East_Q2: 0,
        },
        {
          product: 'Gadget',
          year: 2022,
          North_Q1: 0,
          North_Q2: 0,
          South_Q1: 0,
          South_Q2: 0,
          East_Q1: 1,
          East_Q2: 0,
        },
      ];

      expect(JSON.parse(JSON.stringify(results))).toEqual(expectedResults);
    });

    it('pivots data with multiple aggregations', async () => {
      const pivotedStore = store.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
            name: 'total_sales',
          },
          {
            field: 'units',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
            name: 'total_units',
          },
        ],
        groupBy: ['product', 'year'],
      });

      const results = await pivotedStore.getAll();

      const expectedResults = [
        {
          product: 'Widget',
          year: 2021,
          total_sales_North: 100,
          total_units_North: 10,
          total_sales_South: 120,
          total_units_South: 12,
          total_sales_East: 0,
          total_units_East: 0,
        },
        {
          product: 'Gadget',
          year: 2021,
          total_sales_North: 150,
          total_units_North: 15,
          total_sales_South: 180,
          total_units_South: 18,
          total_sales_East: 0,
          total_units_East: 0,
        },
        {
          product: 'Widget',
          year: 2022,
          total_sales_North: 0,
          total_units_North: 0,
          total_sales_South: 0,
          total_units_South: 0,
          total_sales_East: 90,
          total_units_East: 9,
        },
        {
          product: 'Gadget',
          year: 2022,
          total_sales_North: 0,
          total_units_North: 0,
          total_sales_South: 0,
          total_units_South: 0,
          total_sales_East: 200,
          total_units_East: 20,
        },
      ];

      expect(JSON.parse(JSON.stringify(results))).toEqual(expectedResults);
    });

    it('pivots data with custom aggregation function', async () => {
      const pivotedStore = store.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            // Calculate average as custom aggregation
            aggregation: (values) =>
              values.length > 0
                ? values.reduce((sum, val) => sum + val, 0) / values.length
                : 0,
            name: 'avg_sales',
          },
        ],
        groupBy: ['product'],
      });

      const results = await pivotedStore.getAll();

      expect(results.length).toBe(2); // 2 products

      // Check averages
      const widgetRow = results.find((r) => r.product === 'Widget');
      const gadgetRow = results.find((r) => r.product === 'Gadget');

      expect(widgetRow).toBeDefined();
      expect(gadgetRow).toBeDefined();

      // Widget averages: North = 100, South = 120, East = 90
      expect(widgetRow!['avg_sales_North']).toBe(100);
      expect(widgetRow!['avg_sales_South']).toBe(120);
      expect(widgetRow!['avg_sales_East']).toBe(90);

      // Gadget averages: North = 150, South = 180, East = 200
      expect(gadgetRow!['avg_sales_North']).toBe(150);
      expect(gadgetRow!['avg_sales_South']).toBe(180);
      expect(gadgetRow!['avg_sales_East']).toBe(200);
    });

    it('handles empty dataset correctly', async () => {
      // Create empty table
      const emptyData = {
        product: [] as string[],
        region: [] as string[],
        year: [] as number[],
        quarter: [] as string[],
        sales: [] as number[],
        units: [] as number[],
      };

      const emptyTable = tableFromArrays(emptyData);
      const emptyStore = new ArrowStore(emptyTable, schema);

      const pivotedStore = emptyStore.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
          },
        ],
        groupBy: ['product'],
      });

      const results = await pivotedStore.getAll();
      expect(results.length).toBe(0);
    });

    it('applies row limit correctly', async () => {
      const pivotedStore = store.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
          },
        ],
        groupBy: ['product', 'year'],
        limit: 2,
      });

      const results = await pivotedStore.getAll();
      expect(results.length).toBe(2); // Limited to 2 rows
    });

    it('applies orderBy correctly', async () => {
      const pivotedStore = store.pivot({
        on: 'region',
        using: [
          {
            field: 'sales',
            aggregation: (values) => values.reduce((sum, val) => sum + val, 0),
          },
        ],
        groupBy: ['product', 'year'],
        orderBy: [
          { field: 'product', direction: 'desc' }, // Sort product in descending order
          { field: 'year', direction: 'asc' }, // Then sort year in ascending order
        ],
      });

      const results = await pivotedStore.getAll();

      // First row should be Widget with lowest year
      expect(results[0].product).toBe('Widget');
      expect(results[0].year).toBe(2021);

      // Last row should be Gadget with highest year
      expect(results[results.length - 1].product).toBe('Gadget');
      expect(results[results.length - 1].year).toBe(2022);
    });
  });
});

describe('ArrowStore - Unpivot Operations', () => {
  let store: ArrowStore<any>;
  let table: Table;
  let schema: TableSchema;

  beforeEach(() => {
    table = createRevenueByQuarterTable();
    schema = createRevenueByQuarterSchema();
    store = new ArrowStore(table, schema);
  });

  describe('unpivot', () => {
    it('unpivots data from wide to long format', async () => {
      const unpivotedStore = store.unpivot({
        identifierColumns: ['product'],
        valueColumns: [
          'Q1_2021',
          'Q2_2021',
          'Q3_2021',
          'Q4_2021',
          'Q1_2022',
          'Q2_2022',
        ],
        nameColumn: 'quarter',
        valueColumn: 'revenue',
      });

      const results = await unpivotedStore.getAll();

      expect(results.length).toBe(18); // 3 products × 6 quarters

      // Verify some specific transformations
      const widgetQ1_2021 = results.find(
        (r) => r.product === 'Widget' && r.quarter === 'Q1_2021',
      );
      expect(widgetQ1_2021).toBeDefined();
      expect(widgetQ1_2021?.revenue).toBe(100);

      const gadgetQ2_2022 = results.find(
        (r) => r.product === 'Gadget' && r.quarter === 'Q2_2022',
      );
      expect(gadgetQ2_2022).toBeDefined();
      expect(gadgetQ2_2022?.revenue).toBe(210);

      const accessoryQ3_2021 = results.find(
        (r) => r.product === 'Accessory' && r.quarter === 'Q3_2021',
      );
      expect(accessoryQ3_2021).toBeDefined();
      expect(accessoryQ3_2021?.revenue).toBe(85);
    });

    it('unpivots a subset of columns', async () => {
      const unpivotedStore = store.unpivot({
        identifierColumns: ['product'],
        valueColumns: ['Q1_2021', 'Q2_2021'], // Only unpivot 2021 Q1 and Q2
        nameColumn: 'quarter',
        valueColumn: 'revenue',
      });

      const results = await unpivotedStore.getAll();

      expect(results.length).toBe(6); // 3 products × 2 quarters

      // Verify all products and quarters are represented
      const quarters = new Set(results.map((r) => r.quarter));
      expect(quarters.size).toBe(2);
      expect(quarters.has('Q1_2021')).toBe(true);
      expect(quarters.has('Q2_2021')).toBe(true);
      expect(quarters.has('Q3_2021')).toBe(false); // Not included

      const products = new Set(results.map((r) => r.product));
      expect(products.size).toBe(3);
      expect(products.has('Widget')).toBe(true);
      expect(products.has('Gadget')).toBe(true);
      expect(products.has('Accessory')).toBe(true);
    });

    it('unpivots with multiple identifier columns', async () => {
      // Create more complex data with multiple identifier columns
      const complexData = {
        category: ['Electronics', 'Electronics', 'Furniture', 'Furniture'],
        product: ['Widget', 'Gadget', 'Chair', 'Table'],
        Q1: [100, 150, 200, 250],
        Q2: [120, 180, 220, 270],
      };

      const complexTable = tableFromArrays(complexData);
      const complexSchema = {
        tableName: 'complex',
        columns: [
          { name: 'category', type: 'string' },
          { name: 'product', type: 'string' },
          { name: 'Q1', type: 'float' },
          { name: 'Q2', type: 'float' },
        ],
      };

      const complexStore = new ArrowStore(complexTable, complexSchema);

      const unpivotedStore = complexStore.unpivot({
        identifierColumns: ['category', 'product'], // Multiple id columns
        valueColumns: ['Q1', 'Q2'],
        nameColumn: 'quarter',
        valueColumn: 'sales',
      });

      const results = await unpivotedStore.getAll();

      expect(results.length).toBe(8); // 4 products × 2 quarters

      // Verify identifier columns are preserved
      const electronicWidget = results.find(
        (r) =>
          r.category === 'Electronics' &&
          r.product === 'Widget' &&
          r.quarter === 'Q1',
      );
      expect(electronicWidget).toBeDefined();
      expect(electronicWidget?.sales).toBe(100);

      const furnitureTable = results.find(
        (r) =>
          r.category === 'Furniture' &&
          r.product === 'Table' &&
          r.quarter === 'Q2',
      );
      expect(furnitureTable).toBeDefined();
      expect(furnitureTable?.sales).toBe(270);
    });

    it('handles empty dataset correctly', async () => {
      // Create empty table with same schema
      const emptyData = {
        product: [] as string[],
        Q1_2021: [] as number[],
        Q2_2021: [] as number[],
        Q3_2021: [] as number[],
        Q4_2021: [] as number[],
        Q1_2022: [] as number[],
        Q2_2022: [] as number[],
      };

      const emptyTable = tableFromArrays(emptyData);
      const emptyStore = new ArrowStore(emptyTable, schema);

      const unpivotedStore = emptyStore.unpivot({
        identifierColumns: ['product'],
        valueColumns: [
          'Q1_2021',
          'Q2_2021',
          'Q3_2021',
          'Q4_2021',
          'Q1_2022',
          'Q2_2022',
        ],
        nameColumn: 'quarter',
        valueColumn: 'revenue',
      });

      const results = await unpivotedStore.getAll();
      expect(results.length).toBe(0);
    });

    it('handles rows with null values correctly', async () => {
      // Create data with null values
      const dataWithNulls = {
        product: ['Widget', 'Gadget', 'Accessory'],
        Q1_2021: [100, null, 80],
        Q2_2021: [120, 180, null],
      };

      const nullsTable = tableFromArrays(dataWithNulls);
      const nullsSchema = {
        tableName: 'with_nulls',
        columns: [
          { name: 'product', type: 'string' },
          { name: 'Q1_2021', type: 'float', nullable: true },
          { name: 'Q2_2021', type: 'float', nullable: true },
        ],
      };

      const nullsStore = new ArrowStore(nullsTable, nullsSchema);

      const unpivotedStore = nullsStore.unpivot({
        identifierColumns: ['product'],
        valueColumns: ['Q1_2021', 'Q2_2021'],
        nameColumn: 'quarter',
        valueColumn: 'revenue',
      });

      const results = await unpivotedStore.getAll();

      expect(results.length).toBe(6); // 3 products × 2 quarters

      // Check null values are correctly preserved
      const gadgetQ1 = results.find(
        (r) => r.product === 'Gadget' && r.quarter === 'Q1_2021',
      );
      expect(gadgetQ1).toBeDefined();
      expect(gadgetQ1?.revenue).toBeNull();

      const accessoryQ2 = results.find(
        (r) => r.product === 'Accessory' && r.quarter === 'Q2_2021',
      );
      expect(accessoryQ2).toBeDefined();
      expect(accessoryQ2?.revenue).toBeNull();
    });

    it('can be combined with other operations', async () => {
      // First unpivot data
      const unpivotedStore = store.unpivot({
        identifierColumns: ['product'],
        valueColumns: [
          'Q1_2021',
          'Q2_2021',
          'Q3_2021',
          'Q4_2021',
          'Q1_2022',
          'Q2_2022',
        ],
        nameColumn: 'quarter',
        valueColumn: 'revenue',
      });

      const unpivotedData = (await unpivotedStore.getAll()).map((r) =>
        r.toJSON(),
      );

      expect(unpivotedData).toEqual([
        { product: 'Widget', quarter: 'Q1_2021', revenue: 100 },
        { product: 'Widget', quarter: 'Q2_2021', revenue: 120 },
        { product: 'Widget', quarter: 'Q3_2021', revenue: 110 },
        { product: 'Widget', quarter: 'Q4_2021', revenue: 130 },
        { product: 'Widget', quarter: 'Q1_2022', revenue: 90 },
        { product: 'Widget', quarter: 'Q2_2022', revenue: 95 },
        { product: 'Gadget', quarter: 'Q1_2021', revenue: 150 },
        { product: 'Gadget', quarter: 'Q2_2021', revenue: 180 },
        { product: 'Gadget', quarter: 'Q3_2021', revenue: 160 },
        { product: 'Gadget', quarter: 'Q4_2021', revenue: 190 },
        { product: 'Gadget', quarter: 'Q1_2022', revenue: 200 },
        { product: 'Gadget', quarter: 'Q2_2022', revenue: 210 },
        { product: 'Accessory', quarter: 'Q1_2021', revenue: 80 },
        { product: 'Accessory', quarter: 'Q2_2021', revenue: 90 },
        { product: 'Accessory', quarter: 'Q3_2021', revenue: 85 },
        { product: 'Accessory', quarter: 'Q4_2021', revenue: 95 },
        { product: 'Accessory', quarter: 'Q1_2022', revenue: 100 },
        { product: 'Accessory', quarter: 'Q2_2022', revenue: 105 },
      ]);

      // Then filter unpivoted data
      const filteredStore = unpivotedStore.filter([
        { field: 'product', filter: { op: 'eq', value: 'Widget' } },
        { field: 'quarter', filter: { op: 'contains', value: '2021' } },
      ]);

      const results = await filteredStore.getAll();

      // Should only have Widget entries from 2021
      expect(results.length).toBe(4); // 4 quarters in 2021

      // All results should be for Widget
      expect(results.every((r) => r.product === 'Widget')).toBe(true);

      // All quarters should be from 2021
      expect(results.every((r) => r.quarter.includes('2021'))).toBe(true);
    });
  });
});
