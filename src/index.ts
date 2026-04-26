export { createClient } from "./client.js";
export {
  DuckOrmError,
  QueryExecutionError,
  SchemaConflictError,
  SpatialDisabledError,
  ValidationError,
} from "./errors.js";
export type {
  ColumnDefinition,
  DuckOrm,
  DuckOrmClientOptions,
  EnsureSchemaOptions,
  InsertOptions,
  ResolvedSchema,
  RowObject,
  SchemaMode,
  SelectOptions,
  SpatialApi,
  SpatialColumnConfig,
  SpatialInitOptions,
  SpatialInputFormat,
  SpatialPredicate,
} from "./types.js";
