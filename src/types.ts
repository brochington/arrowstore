export interface ColumnSchema {
  name: string;
  type: string;
  nullable?: boolean;
  metadata?: Map<string, any>;
}

export interface TableSchema {
  tableName: string;
  columns: ColumnSchema[];
}
