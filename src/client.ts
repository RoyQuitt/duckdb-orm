import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import type { DuckDBValue } from "@duckdb/node-api";
import { QueryExecutionError, ValidationError } from "./errors.js";
import {
  buildAlterColumnSql,
  buildCreateTableSql,
  compareAgainstExisting,
  deriveColumns,
  getExistingColumns,
  getSpatialColumnsFromConfig,
  normalizeRows,
  tableExists,
} from "./schema.js";
import { placeholder, quoteIdentifier, quoteTable } from "./sql.js";
import {
  buildSpatialRuntime,
  createSpatialApi,
  geometrySelectExpression,
  spatialInputExpression,
} from "./spatial.js";
import type {
  ColumnDefinition,
  DuckOrm,
  DuckOrmClientOptions,
  EnsureSchemaOptions,
  InsertOptions,
  ResolvedSchema,
  RowObject,
  SelectOptions,
  SpatialInitOptions,
  SpatialPredicate,
} from "./types.js";

const DEFAULT_SPATIAL_OPTIONS: Required<Pick<SpatialInitOptions, "enabled" | "installIfMissing" | "loadStrategy">> =
  {
    enabled: false,
    installIfMissing: false,
    loadStrategy: "onInit",
  };

export async function createClient(options: DuckOrmClientOptions = {}): Promise<DuckOrm> {
  const path = options.path ?? ":memory:";
  const instance = await DuckDBInstance.fromCache(path, options.config ?? {});
  const connection = await instance.connect();
  const client = new DuckOrmClient(connection, options);
  await client.initialize();
  return client;
}

class DuckOrmClient implements DuckOrm {
  public readonly spatial;
  private readonly connection: DuckDBConnection;
  private readonly options: DuckOrmClientOptions;
  private readonly spatialOptions: Required<Pick<SpatialInitOptions, "enabled" | "installIfMissing" | "loadStrategy">> &
    Pick<SpatialInitOptions, "repository">;
  private readonly spatialRuntime;
  private readonly queryInternalFn;

  constructor(connection: DuckDBConnection, options: DuckOrmClientOptions) {
    this.connection = connection;
    this.options = options;
    this.spatialOptions = {
      ...DEFAULT_SPATIAL_OPTIONS,
      ...(options.spatial ?? {}),
    };
    this.spatialRuntime = buildSpatialRuntime(options.spatial, async (sql, params) => {
      await this.run(sql, params);
    });
    this.spatial = createSpatialApi(this.spatialRuntime);
    this.queryInternalFn = this.queryInternal.bind(this);
  }

  async initialize(): Promise<void> {
    if (this.spatialOptions.enabled && this.spatialOptions.loadStrategy === "onInit") {
      await this.spatialRuntime.ensureLoaded();
    }
  }

  async ensureSchema(sample: RowObject | RowObject[], options: EnsureSchemaOptions): Promise<ResolvedSchema> {
    const rows = normalizeRows(sample);
    const mode = options.mode ?? "evolve";
    const columns = deriveColumns(rows, options);

    const exists = await tableExists(this.queryInternalFn, options.table);
    if (!exists) {
      await this.exec(buildCreateTableSql(options.table, columns));
      return {
        table: options.table,
        mode,
        columns,
        created: true,
        alteredColumns: [],
      };
    }

    const existing = await getExistingColumns(this.queryInternalFn, options.table);
    const compared = compareAgainstExisting(mode, columns, existing);
    for (const missingColumn of compared.missing) {
      await this.exec(buildAlterColumnSql(options.table, missingColumn));
    }

    return {
      table: options.table,
      mode,
      columns,
      created: false,
      alteredColumns: compared.missing.map((column) => column.name),
    };
  }

  async insertObjects(table: string, rows: RowObject[], options: InsertOptions = {}): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    const schema = await this.ensureSchema(rows, { table, ...options });
    const nested = options.nested ?? "json";
    const spatialColumns = getSpatialColumnsFromConfig(options.spatialColumns);
    const firstRow = normalizeForInsert(rows[0], nested);
    const columns = Object.keys(firstRow);
    if (columns.length === 0) {
      throw new ValidationError("Cannot insert empty row.");
    }

    for (let i = 1; i < rows.length; i++) {
      const keys = Object.keys(normalizeForInsert(rows[i], nested)).sort();
      const base = [...columns].sort();
      if (keys.join("|") !== base.join("|")) {
        throw new ValidationError(
          "All rows in a single insertObjects call must have the same shape. Use separate batches or evolve schema first."
        );
      }
    }

    if (spatialColumns.size > 0) {
      await this.spatialRuntime.ensureLoaded();
    }

    const columnSet = new Set(schema.columns.map((column) => column.name));
    for (const column of columns) {
      if (!columnSet.has(column)) {
        throw new ValidationError(`Column "${column}" is not present in resolved schema.`);
      }
    }

    const tableSql = quoteTable(table);
    const columnSql = columns.map(quoteIdentifier).join(", ");
    const values: unknown[] = [];
    const valueRows: string[] = [];
    let paramIndex = 1;

    for (const row of rows) {
      const normalized = normalizeForInsert(row, nested);
      const rowExpr: string[] = [];
      for (const column of columns) {
        const raw = normalized[column];
        const normalizedValue = normalizeValue(raw);
        values.push(normalizedValue);
        const placeholderSql = placeholder(paramIndex++);
        if (spatialColumns.has(column)) {
          const input = options.spatialColumns?.[column]?.input ?? "wkt";
          rowExpr.push(spatialInputExpression(input, placeholderSql));
        } else {
          rowExpr.push(placeholderSql);
        }
      }
      valueRows.push(`(${rowExpr.join(", ")})`);
    }

    const sql = `INSERT INTO ${tableSql} (${columnSql}) VALUES ${valueRows.join(", ")}`;
    await this.exec(sql, values);
  }

  async select<T extends RowObject = RowObject>(
    table: string,
    where: Record<string, unknown> = {},
    options: SelectOptions = {}
  ): Promise<T[]> {
    if (options.spatial && options.spatial.length > 0) {
      await this.spatialRuntime.ensureLoaded();
    }

    const params: unknown[] = [];
    const whereClauses: string[] = [];
    let paramIndex = 1;

    for (const [column, value] of Object.entries(where)) {
      whereClauses.push(`${quoteIdentifier(column)} = ${placeholder(paramIndex++)}`);
      params.push(normalizeValue(value));
    }

    for (const predicate of options.spatial ?? []) {
      const built = buildSpatialPredicate(predicate, paramIndex);
      paramIndex = built.nextParam;
      whereClauses.push(built.sql);
      params.push(...built.params);
    }

    const whereSql = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(" AND ")}` : "";
    const limitSql = options.limit !== undefined ? ` LIMIT ${options.limit}` : "";
    const offsetSql = options.offset !== undefined ? ` OFFSET ${options.offset}` : "";
    const selectList = buildSelectList(options.geometryColumns, options.geometryOutput ?? "raw");
    const sql = `SELECT ${selectList} FROM ${quoteTable(table)}${whereSql}${limitSql}${offsetSql}`;
    return await this.query<T>(sql, params);
  }

  async exec(sql: string, params?: unknown[] | Record<string, unknown>): Promise<void> {
    await this.run(sql, params);
  }

  async query<T extends RowObject = RowObject>(
    sql: string,
    params?: unknown[] | Record<string, unknown>
  ): Promise<T[]> {
    return await this.queryInternal<T>(sql, params as unknown[] | undefined);
  }

  async transaction<R>(fn: (tx: DuckOrm) => Promise<R>): Promise<R> {
    await this.exec("BEGIN");
    try {
      const result = await fn(this);
      await this.exec("COMMIT");
      return result;
    } catch (error) {
      await this.exec("ROLLBACK");
      throw error;
    }
  }

  async close(): Promise<void> {
    this.connection.closeSync();
  }

  private async run(sql: string, params?: unknown[] | Record<string, unknown>): Promise<void> {
    try {
      if (params !== undefined) {
        await this.connection.run(sql, toDuckDbParams(params));
      } else {
        await this.connection.run(sql);
      }
    } catch (error) {
      throw new QueryExecutionError(`Failed to execute SQL: ${sql}`, error);
    }
  }

  private readonly queryInternal = async <T>(
    sql: string,
    params?: unknown[] | Record<string, unknown>
  ): Promise<T[]> => {
    try {
      const reader =
        params !== undefined
          ? await this.connection.runAndReadAll(sql, toDuckDbParams(params))
          : await this.connection.runAndReadAll(sql);
      return reader.getRowObjectsJson() as T[];
    } catch (error) {
      throw new QueryExecutionError(`Failed to query SQL: ${sql}`, error);
    }
  };
}

function toDuckDbParams(params: unknown[] | Record<string, unknown>): DuckDBValue[] | Record<string, DuckDBValue> {
  if (Array.isArray(params)) {
    return params.map((value) => normalizeDuckDbParam(value));
  }
  const out: Record<string, DuckDBValue> = {};
  for (const [key, value] of Object.entries(params)) {
    out[key] = normalizeDuckDbParam(value);
  }
  return out;
}

function normalizeDuckDbParam(value: unknown): DuckDBValue {
  const normalized = normalizeValue(value);
  return normalized as DuckDBValue;
}

function normalizeForInsert(row: RowObject, nested: "json" | "flatten"): RowObject {
  if (nested === "json") {
    return row;
  }
  return flattenObject(row);
}

function flattenObject(input: RowObject, prefix = "", output: RowObject = {}): RowObject {
  for (const [key, value] of Object.entries(input)) {
    const nextKey = prefix ? `${prefix}_${key}` : key;
    if (value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date)) {
      flattenObject(value as RowObject, nextKey, output);
      continue;
    }
    output[nextKey] = value;
  }
  return output;
}

function normalizeValue(value: unknown): unknown {
  if (value === undefined) {
    return null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return JSON.stringify(value);
  }
  return value;
}

function buildSelectList(
  geometryColumns: string[] | undefined,
  geometryOutput: "raw" | "wkt" | "geojson"
): string {
  if (!geometryColumns || geometryColumns.length === 0) {
    return "*";
  }

  if (geometryOutput === "raw") {
    return "*";
  }

  const transformed = geometryColumns.map((column) => geometrySelectExpression(column, geometryOutput));
  const withoutDuplicates = Array.from(new Set(transformed));
  return `*, ${withoutDuplicates.join(", ")}`;
}

function buildSpatialPredicate(
  predicate: SpatialPredicate,
  startParam: number
): { sql: string; params: unknown[]; nextParam: number } {
  const columnSql = quoteIdentifier(predicate.column);
  const geometryParam = placeholder(startParam);
  const geometryExpr = spatialInputExpression(predicate.input, geometryParam);

  if (predicate.kind === "intersects") {
    return {
      sql: `ST_Intersects(${columnSql}, ${geometryExpr})`,
      params: [predicate.geometry],
      nextParam: startParam + 1,
    };
  }

  if (predicate.distance === undefined || Number.isNaN(predicate.distance)) {
    throw new ValidationError("Spatial predicate dwithin requires a numeric distance.");
  }
  const distanceParam = placeholder(startParam + 1);
  return {
    sql: `ST_DWithin(${columnSql}, ${geometryExpr}, ${distanceParam})`,
    params: [predicate.geometry, predicate.distance],
    nextParam: startParam + 2,
  };
}
