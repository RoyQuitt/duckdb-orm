import { QueryExecutionError, SpatialDisabledError } from "./errors.js";
import { asStringLiteral, quoteIdentifier } from "./sql.js";
import type {
  SpatialApi,
  SpatialInitOptions,
  SpatialInputFormat,
  SpatialPredicate,
} from "./types.js";

export interface SpatialRuntime {
  ensureLoaded(): Promise<void>;
  isEnabled(): boolean;
  isReady(): Promise<boolean>;
}

export function buildSpatialRuntime(
  options: SpatialInitOptions | undefined,
  run: (sql: string, params?: unknown[]) => Promise<void>
): SpatialRuntime {
  const enabled = options?.enabled ?? false;
  const installIfMissing = options?.installIfMissing ?? false;
  const repository = options?.repository;
  let loaded = false;

  async function ensureLoaded(): Promise<void> {
    if (!enabled) {
      throw new SpatialDisabledError();
    }
    if (loaded) {
      return;
    }

    try {
      await run("LOAD spatial");
      loaded = true;
      return;
    } catch (error) {
      if (!installIfMissing) {
        throw new QueryExecutionError(
          "Failed to load DuckDB spatial extension. Enable installIfMissing or install extension manually.",
          error
        );
      }
    }

    const installSql = repository
      ? `INSTALL spatial FROM ${normalizeRepository(repository)}`
      : "INSTALL spatial";
    await run(installSql);
    await run("LOAD spatial");
    loaded = true;
  }

  async function isReady(): Promise<boolean> {
    if (!enabled) {
      return false;
    }
    if (loaded) {
      return true;
    }
    try {
      await run("LOAD spatial");
      loaded = true;
      return true;
    } catch {
      return false;
    }
  }

  return {
    ensureLoaded,
    isEnabled: () => enabled,
    isReady,
  };
}

export function createSpatialApi(runtime: SpatialRuntime): SpatialApi {
  return {
    isEnabled: runtime.isEnabled,
    isReady: runtime.isReady,
    ensureLoaded: runtime.ensureLoaded,
    whereIntersects(column: string, geometry: unknown, input: SpatialInputFormat = "wkt"): SpatialPredicate {
      return {
        kind: "intersects",
        column,
        geometry,
        input,
      };
    },
    whereDWithin(
      column: string,
      geometry: unknown,
      distance: number,
      input: SpatialInputFormat = "wkt"
    ): SpatialPredicate {
      return {
        kind: "dwithin",
        column,
        geometry,
        input,
        distance,
      };
    },
  };
}

export function spatialInputExpression(input: SpatialInputFormat, paramPlaceholder: string): string {
  switch (input) {
    case "wkt":
      return `ST_GeomFromText(${paramPlaceholder})`;
    case "geojson":
      return `ST_GeomFromGeoJSON(${paramPlaceholder})`;
    case "wkb":
      return `ST_GeomFromWKB(${paramPlaceholder})`;
    default:
      return paramPlaceholder;
  }
}

export function geometrySelectExpression(column: string, output: "raw" | "wkt" | "geojson"): string {
  const col = quoteIdentifier(column);
  if (output === "wkt") {
    return `ST_AsText(${col}) AS ${col}`;
  }
  if (output === "geojson") {
    return `ST_AsGeoJSON(${col}) AS ${col}`;
  }
  return col;
}

function normalizeRepository(repository: string): string {
  if (repository === "core" || repository === "core_nightly") {
    return repository;
  }
  return asStringLiteral(repository);
}
