import { describe, expect, test, vi } from "vitest";
import { SpatialDisabledError } from "../../src/errors.js";
import { assertIdentifier, asStringLiteral, quoteIdentifier } from "../../src/sql.js";
import {
  buildSpatialRuntime,
  createSpatialApi,
  geometrySelectExpression,
  spatialInputExpression,
} from "../../src/spatial.js";

describe("sql helpers", () => {
  test("identifier validation rejects unsafe names", () => {
    expect(() => assertIdentifier("users;DROP TABLE x", "table")).toThrow();
  });

  test("quoteIdentifier quotes valid names", () => {
    expect(quoteIdentifier("users")).toBe('"users"');
  });

  test("asStringLiteral escapes single quotes", () => {
    expect(asStringLiteral("a'b")).toBe("'a''b'");
  });
});

describe("spatial helpers", () => {
  test("spatial input expression maps by format", () => {
    expect(spatialInputExpression("wkt", "$1")).toBe("ST_GeomFromText($1)");
    expect(spatialInputExpression("geojson", "$1")).toBe("ST_GeomFromGeoJSON($1)");
    expect(spatialInputExpression("wkb", "$1")).toBe("ST_GeomFromWKB($1)");
  });

  test("geometry select expression maps output format", () => {
    expect(geometrySelectExpression("geom", "raw")).toBe('"geom"');
    expect(geometrySelectExpression("geom", "wkt")).toBe('ST_AsText("geom") AS "geom"');
    expect(geometrySelectExpression("geom", "geojson")).toBe('ST_AsGeoJSON("geom") AS "geom"');
  });

  test("disabled runtime throws on ensureLoaded", async () => {
    const run = vi.fn(async () => {});
    const runtime = buildSpatialRuntime({ enabled: false }, run);
    await expect(runtime.ensureLoaded()).rejects.toThrow(SpatialDisabledError);
    expect(run).not.toHaveBeenCalled();
  });

  test("api predicate builders create expected shape", () => {
    const runtime = buildSpatialRuntime({ enabled: true }, async () => {});
    const api = createSpatialApi(runtime);

    expect(api.whereIntersects("geom", "POINT(0 0)", "wkt")).toEqual({
      kind: "intersects",
      column: "geom",
      geometry: "POINT(0 0)",
      input: "wkt",
    });

    expect(api.whereDWithin("geom", "POINT(0 0)", 10, "wkt")).toEqual({
      kind: "dwithin",
      column: "geom",
      geometry: "POINT(0 0)",
      input: "wkt",
      distance: 10,
    });
  });
});
