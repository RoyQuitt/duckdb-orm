import { SchemaConflictError, ValidationError } from "./errors.js";
import { quoteIdentifier, quoteTable } from "./sql.js";
import type {
  ColumnDefinition,
  EnsureSchemaOptions,
  RowObject,
  SchemaMode,
  SpatialColumnConfig,
} from "./types.js";

interface ExistingColumn {
  name: string;
  type: string;
  nullable: boolean;
}

type ScalarType = "VARCHAR" | "DOUBLE" | "BIGINT" | "BOOLEAN" | "TIMESTAMP" | "JSON" | "GEOMETRY";

export function normalizeRows(sample: RowObject | RowObject[]): RowObject[] {
  const rows = Array.isArray(sample) ? sample : [sample];
  if (rows.length === 0) {
    throw new ValidationError("At least one row is required for schema derivation.");
  }
  return rows;
}

export function deriveColumns(
  rows: RowObject[],
  options: Pick<EnsureSchemaOptions, "nested" | "inferIntegersAsBigInt" | "spatialColumns">
): ColumnDefinition[] {
  const nested = options.nested ?? "json";
  const spatialColumns = options.spatialColumns ?? {};
  const inferIntegersAsBigInt = options.inferIntegersAsBigInt ?? false;

  const accumulator = new Map<string, { types: Set<ScalarType>; nullable: boolean }>();

  for (const row of rows) {
    const normalized = nested === "flatten" ? flattenObject(row) : row;
    for (const [key, value] of Object.entries(normalized)) {
      const entry = accumulator.get(key) ?? { types: new Set<ScalarType>(), nullable: false };
      const spatialType = spatialColumns[key];
      if (spatialType) {
        entry.types.add("GEOMETRY");
      } else {
        const inferred = inferType(value, inferIntegersAsBigInt);
        if (inferred === null) {
          entry.nullable = true;
        } else {
          entry.types.add(inferred);
        }
      }
      accumulator.set(key, entry);
    }
  }

  if (accumulator.size === 0) {
    throw new ValidationError("Could not derive schema from empty object shape.");
  }

  const columns: ColumnDefinition[] = [];
  for (const [name, state] of accumulator.entries()) {
    columns.push({
      name,
      type: mergeTypes(name, state.types),
      nullable: state.nullable || state.types.size === 0,
    });
  }

  columns.sort((a, b) => a.name.localeCompare(b.name));
  return columns;
}

export function buildCreateTableSql(table: string, columns: ColumnDefinition[]): string {
  const tableSql = quoteTable(table);
  const columnsSql = columns
    .map((column) => {
      const nullableSql = column.nullable ? "" : " NOT NULL";
      return `${quoteIdentifier(column.name)} ${column.type}${nullableSql}`;
    })
    .join(", ");
  return `CREATE TABLE ${tableSql} (${columnsSql})`;
}

export function buildAlterColumnSql(table: string, column: ColumnDefinition): string {
  const tableSql = quoteTable(table);
  return `ALTER TABLE ${tableSql} ADD COLUMN ${quoteIdentifier(column.name)} ${column.type}`;
}

export function compareAgainstExisting(
  mode: SchemaMode,
  inferred: ColumnDefinition[],
  existing: ExistingColumn[]
): { missing: ColumnDefinition[] } {
  const existingByName = new Map(existing.map((column) => [column.name, column]));
  const missing: ColumnDefinition[] = [];

  for (const column of inferred) {
    const existingColumn = existingByName.get(column.name);
    if (!existingColumn) {
      if (mode === "strict") {
        throw new SchemaConflictError(
          `Schema drift detected: missing column "${column.name}" in strict mode.`
        );
      }
      missing.push(column);
      continue;
    }
    if (!areTypesCompatible(existingColumn.type, column.type)) {
      throw new SchemaConflictError(
        `Type conflict for column "${column.name}": existing=${existingColumn.type}, inferred=${column.type}.`
      );
    }
  }

  if (mode === "strict") {
    const inferredNames = new Set(inferred.map((column) => column.name));
    for (const existingColumn of existing) {
      if (!inferredNames.has(existingColumn.name)) {
        throw new SchemaConflictError(
          `Schema drift detected: existing column "${existingColumn.name}" was not present in input in strict mode.`
        );
      }
    }
  }

  return { missing };
}

export async function tableExists(
  query: <T>(sql: string, params?: unknown[]) => Promise<T[]>,
  table: string
): Promise<boolean> {
  const rows = await query<{ exists_count: number }>(
    `SELECT COUNT(*)::INTEGER AS exists_count
     FROM information_schema.tables
     WHERE table_schema = current_schema()
       AND table_name = $1`,
    [table]
  );
  return (rows[0]?.exists_count ?? 0) > 0;
}

export async function getExistingColumns(
  query: <T>(sql: string, params?: unknown[]) => Promise<T[]>,
  table: string
): Promise<ExistingColumn[]> {
  const rows = await query<{
    column_name: string;
    data_type: string;
    is_nullable: "YES" | "NO";
  }>(
    `SELECT column_name, data_type, is_nullable
     FROM information_schema.columns
     WHERE table_schema = current_schema()
       AND table_name = $1
     ORDER BY ordinal_position`,
    [table]
  );

  return rows.map((row) => ({
    name: row.column_name,
    type: normalizeDbType(row.data_type),
    nullable: row.is_nullable === "YES",
  }));
}

export function getSpatialColumnsFromConfig(
  spatialColumns: Record<string, SpatialColumnConfig> | undefined
): Set<string> {
  if (!spatialColumns) {
    return new Set<string>();
  }
  return new Set(Object.keys(spatialColumns));
}

function inferType(value: unknown, inferIntegersAsBigInt: boolean): ScalarType | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    return "VARCHAR";
  }
  if (typeof value === "boolean") {
    return "BOOLEAN";
  }
  if (typeof value === "bigint") {
    return "BIGINT";
  }
  if (typeof value === "number") {
    if (inferIntegersAsBigInt && Number.isInteger(value)) {
      return "BIGINT";
    }
    return "DOUBLE";
  }
  if (value instanceof Date) {
    return "TIMESTAMP";
  }
  if (Array.isArray(value) || typeof value === "object") {
    return "JSON";
  }
  return "VARCHAR";
}

function mergeTypes(columnName: string, types: Set<ScalarType>): ScalarType {
  if (types.size === 0) {
    return "VARCHAR";
  }
  if (types.size === 1) {
    return [...types][0];
  }

  if (types.has("VARCHAR")) {
    return "VARCHAR";
  }
  if (types.has("JSON")) {
    return "JSON";
  }
  if (types.has("DOUBLE") && types.has("BIGINT") && types.size === 2) {
    return "DOUBLE";
  }

  throw new SchemaConflictError(
    `Could not merge inferred types for column "${columnName}": ${[...types].join(", ")}`
  );
}

function normalizeDbType(type: string): string {
  return type.toUpperCase();
}

function areTypesCompatible(existing: string, inferred: string): boolean {
  if (existing === inferred) {
    return true;
  }
  if (existing === "DOUBLE" && inferred === "BIGINT") {
    return true;
  }
  return false;
}

function flattenObject(
  input: RowObject,
  prefix = "",
  output: RowObject = {}
): RowObject {
  for (const [key, value] of Object.entries(input)) {
    const currentKey = prefix ? `${prefix}_${key}` : key;
    if (value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date)) {
      flattenObject(value as RowObject, currentKey, output);
      continue;
    }
    output[currentKey] = value;
  }
  return output;
}
