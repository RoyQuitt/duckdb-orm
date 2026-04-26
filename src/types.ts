export type SchemaMode = "strict" | "evolve";

export type NestedMode = "json" | "flatten";

export type SpatialInputFormat = "wkt" | "geojson" | "wkb";

export interface SpatialColumnConfig {
  kind: "geometry";
  input: SpatialInputFormat;
}

export interface EnsureSchemaOptions {
  table: string;
  mode?: SchemaMode;
  nested?: NestedMode;
  inferIntegersAsBigInt?: boolean;
  spatialColumns?: Record<string, SpatialColumnConfig>;
}

export interface InsertOptions extends Omit<EnsureSchemaOptions, "table"> {}

export interface SelectOptions {
  limit?: number;
  offset?: number;
  spatial?: SpatialPredicate[];
  geometryColumns?: string[];
  geometryOutput?: "raw" | "wkt" | "geojson";
}

export interface SpatialInitOptions {
  enabled?: boolean;
  installIfMissing?: boolean;
  repository?: "core" | "core_nightly" | string;
  loadStrategy?: "onInit" | "lazy";
}

export interface DuckOrmClientOptions {
  path?: string;
  config?: Record<string, string>;
  spatial?: SpatialInitOptions;
}

export interface ColumnDefinition {
  name: string;
  type: string;
  nullable: boolean;
}

export interface ResolvedSchema {
  table: string;
  mode: SchemaMode;
  columns: ColumnDefinition[];
  created: boolean;
  alteredColumns: string[];
}

export type PrimitiveValue = string | number | boolean | bigint | Date | null;

export type RowObject = Record<string, unknown>;

export interface SpatialPredicate {
  readonly kind: "intersects" | "dwithin";
  readonly column: string;
  readonly geometry: unknown;
  readonly input: SpatialInputFormat;
  readonly distance?: number;
}

export interface SpatialApi {
  isEnabled(): boolean;
  isReady(): Promise<boolean>;
  ensureLoaded(): Promise<void>;
  whereIntersects(
    column: string,
    geometry: unknown,
    input?: SpatialInputFormat
  ): SpatialPredicate;
  whereDWithin(
    column: string,
    geometry: unknown,
    distance: number,
    input?: SpatialInputFormat
  ): SpatialPredicate;
}

export interface DuckOrm {
  ensureSchema(sample: RowObject | RowObject[], options: EnsureSchemaOptions): Promise<ResolvedSchema>;
  insertObjects(table: string, rows: RowObject[], options?: InsertOptions): Promise<void>;
  select<T extends RowObject = RowObject>(
    table: string,
    where?: Record<string, unknown>,
    options?: SelectOptions
  ): Promise<T[]>;
  exec(sql: string, params?: unknown[] | Record<string, unknown>): Promise<void>;
  query<T extends RowObject = RowObject>(
    sql: string,
    params?: unknown[] | Record<string, unknown>
  ): Promise<T[]>;
  transaction<R>(fn: (tx: DuckOrm) => Promise<R>): Promise<R>;
  close(): Promise<void>;
  spatial: SpatialApi;
}
