import { describe, expect, test } from "vitest";
import {
  buildCreateTableSql,
  compareAgainstExisting,
  deriveColumns,
  normalizeRows,
} from "../../src/schema.js";
import { SchemaConflictError, ValidationError } from "../../src/errors.js";

describe("schema utilities", () => {
  test("normalizeRows rejects empty array", () => {
    expect(() => normalizeRows([])).toThrow(ValidationError);
  });

  test("deriveColumns infers primitive and json types", () => {
    const columns = deriveColumns(
      [{ id: 1, name: "ada", active: true, meta: { role: "admin" }, tags: ["a"] }],
      { nested: "json" }
    );
    expect(columns).toEqual([
      { name: "active", type: "BOOLEAN", nullable: false },
      { name: "id", type: "DOUBLE", nullable: false },
      { name: "meta", type: "JSON", nullable: false },
      { name: "name", type: "VARCHAR", nullable: false },
      { name: "tags", type: "JSON", nullable: false },
    ]);
  });

  test("deriveColumns flattens nested objects in flatten mode", () => {
    const columns = deriveColumns([{ profile: { city: "TLV", zip: 123 } }], { nested: "flatten" });
    expect(columns).toEqual([
      { name: "profile_city", type: "VARCHAR", nullable: false },
      { name: "profile_zip", type: "DOUBLE", nullable: false },
    ]);
  });

  test("deriveColumns supports spatial column override", () => {
    const columns = deriveColumns(
      [{ geom: "POINT(1 1)", id: 1 }],
      {
        spatialColumns: {
          geom: { kind: "geometry", input: "wkt" },
        },
      }
    );
    expect(columns).toEqual([
      { name: "geom", type: "GEOMETRY", nullable: false },
      { name: "id", type: "DOUBLE", nullable: false },
    ]);
  });

  test("compareAgainstExisting detects strict drift", () => {
    expect(() =>
      compareAgainstExisting(
        "strict",
        [{ name: "id", type: "DOUBLE", nullable: false }],
        [
          { name: "id", type: "DOUBLE", nullable: false },
          { name: "name", type: "VARCHAR", nullable: true },
        ]
      )
    ).toThrow(SchemaConflictError);
  });

  test("compareAgainstExisting allows evolve mode missing columns", () => {
    const res = compareAgainstExisting(
      "evolve",
      [
        { name: "id", type: "DOUBLE", nullable: false },
        { name: "name", type: "VARCHAR", nullable: true },
      ],
      [{ name: "id", type: "DOUBLE", nullable: false }]
    );
    expect(res.missing).toEqual([{ name: "name", type: "VARCHAR", nullable: true }]);
  });

  test("buildCreateTableSql quotes identifiers", () => {
    const sql = buildCreateTableSql("users", [
      { name: "id", type: "DOUBLE", nullable: false },
      { name: "name", type: "VARCHAR", nullable: true },
    ]);
    expect(sql).toBe('CREATE TABLE "users" ("id" DOUBLE NOT NULL, "name" VARCHAR)');
  });
});
