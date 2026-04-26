import assert from "node:assert/strict";
import { mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { createClient } from "../dist/client.js";
import { SchemaConflictError, SpatialDisabledError, ValidationError } from "../dist/errors.js";
import {
  buildCreateTableSql,
  compareAgainstExisting,
  deriveColumns,
  normalizeRows,
} from "../dist/schema.js";
import { assertIdentifier, asStringLiteral, quoteIdentifier } from "../dist/sql.js";
import {
  buildSpatialRuntime,
  createSpatialApi,
  geometrySelectExpression,
  spatialInputExpression,
} from "../dist/spatial.js";

const TMP_DIR = path.resolve(process.cwd(), "test", ".tmp");
await mkdir(TMP_DIR, { recursive: true });

const tests = [];

function it(name, fn) {
  tests.push({ name, fn });
}

function makeDbPath(name) {
  return path.join(TMP_DIR, `${name}-${randomUUID()}.duckdb`);
}

it("normalizeRows rejects empty array", () => {
  assert.throws(() => normalizeRows([]), ValidationError);
});

it("deriveColumns infers primitive/json and flatten works", () => {
  const columnsJson = deriveColumns(
    [{ id: 1, name: "ada", active: true, meta: { role: "admin" }, tags: ["a"] }],
    { nested: "json" }
  );
  assert.deepEqual(columnsJson, [
    { name: "active", type: "BOOLEAN", nullable: false },
    { name: "id", type: "DOUBLE", nullable: false },
    { name: "meta", type: "JSON", nullable: false },
    { name: "name", type: "VARCHAR", nullable: false },
    { name: "tags", type: "JSON", nullable: false },
  ]);

  const columnsFlat = deriveColumns([{ profile: { city: "TLV", zip: 123 } }], { nested: "flatten" });
  assert.deepEqual(columnsFlat, [
    { name: "profile_city", type: "VARCHAR", nullable: false },
    { name: "profile_zip", type: "DOUBLE", nullable: false },
  ]);
});

it("spatial schema override maps to GEOMETRY", () => {
  const columns = deriveColumns(
    [{ geom: "POINT(1 1)", id: 1 }],
    { spatialColumns: { geom: { kind: "geometry", input: "wkt" } } }
  );
  assert.deepEqual(columns, [
    { name: "geom", type: "GEOMETRY", nullable: false },
    { name: "id", type: "DOUBLE", nullable: false },
  ]);
});

it("schema compare strict/evolve behavior", () => {
  assert.throws(
    () =>
      compareAgainstExisting(
        "strict",
        [{ name: "id", type: "DOUBLE", nullable: false }],
        [
          { name: "id", type: "DOUBLE", nullable: false },
          { name: "name", type: "VARCHAR", nullable: true },
        ]
      ),
    SchemaConflictError
  );

  const res = compareAgainstExisting(
    "evolve",
    [
      { name: "id", type: "DOUBLE", nullable: false },
      { name: "name", type: "VARCHAR", nullable: true },
    ],
    [{ name: "id", type: "DOUBLE", nullable: false }]
  );
  assert.deepEqual(res.missing, [{ name: "name", type: "VARCHAR", nullable: true }]);
});

it("sql and spatial helper behavior", async () => {
  assert.equal(buildCreateTableSql("users", [{ name: "id", type: "DOUBLE", nullable: false }]), 'CREATE TABLE "users" ("id" DOUBLE NOT NULL)');
  assert.throws(() => assertIdentifier("users;DROP TABLE x", "table"));
  assert.equal(quoteIdentifier("users"), '"users"');
  assert.equal(asStringLiteral("a'b"), "'a''b'");
  assert.equal(spatialInputExpression("wkt", "$1"), "ST_GeomFromText($1)");
  assert.equal(geometrySelectExpression("geom", "geojson"), 'ST_AsGeoJSON("geom") AS "geom"');

  const disabledRuntime = buildSpatialRuntime({ enabled: false }, async () => {});
  await assert.rejects(disabledRuntime.ensureLoaded(), SpatialDisabledError);

  const api = createSpatialApi(buildSpatialRuntime({ enabled: true }, async () => {}));
  assert.deepEqual(api.whereIntersects("geom", "POINT(0 0)", "wkt"), {
    kind: "intersects",
    column: "geom",
    geometry: "POINT(0 0)",
    input: "wkt",
  });
});

it("connects and runs basic query", async () => {
  const db = await createClient({ path: makeDbPath("basic-query") });
  try {
    const rows = await db.query("SELECT 42::INTEGER AS value");
    assert.deepEqual(rows, [{ value: 42 }]);
  } finally {
    await db.close();
  }
});

it("auto derives schema, inserts and selects", async () => {
  const db = await createClient({ path: makeDbPath("derive-insert-select") });
  try {
    await db.insertObjects("users", [{ id: 1, name: "Ada", active: true }], { mode: "evolve" });
    const users = await db.select("users", { id: 1 });
    assert.equal(users.length, 1);
    assert.equal(users[0].name, "Ada");
  } finally {
    await db.close();
  }
});

it("schema evolve/strict and mixed-shape validation", async () => {
  const db = await createClient({ path: makeDbPath("schema-behavior") });
  try {
    await db.insertObjects("events", [{ id: 1 }], { mode: "evolve" });
    const result = await db.ensureSchema([{ id: 1, title: "launch" }], { table: "events", mode: "evolve" });
    assert.equal(result.alteredColumns.includes("title"), true);
    await assert.rejects(
      db.ensureSchema([{ id: 1, title: "x", extra: true }], { table: "events", mode: "strict" }),
      SchemaConflictError
    );

    await assert.rejects(
      db.insertObjects(
        "logs",
        [
          { id: 1, message: "a" },
          { id: 2 },
        ],
        { mode: "evolve" }
      ),
      ValidationError
    );
  } finally {
    await db.close();
  }
});

it("transaction commit and rollback", async () => {
  const db = await createClient({ path: makeDbPath("transaction") });
  try {
    await db.ensureSchema([{ id: 1, label: "x" }], { table: "tx_rows", mode: "evolve" });

    await db.transaction(async (tx) => {
      await tx.insertObjects("tx_rows", [{ id: 1, label: "ok" }], { mode: "evolve" });
    });
    let rows = await db.select("tx_rows");
    assert.equal(rows.length, 1);

    await assert.rejects(
      db.transaction(async (tx) => {
        await tx.insertObjects("tx_rows", [{ id: 2, label: "rollback" }], { mode: "evolve" });
        throw new Error("force rollback");
      }),
      /force rollback/
    );
    rows = await db.select("tx_rows");
    assert.equal(rows.length, 1);
  } finally {
    await db.close();
  }
});

it("injection safety and identifier validation", async () => {
  const db = await createClient({ path: makeDbPath("injection") });
  try {
    const payload = "x'); DROP TABLE users; --";
    await db.insertObjects("users", [{ id: 1, name: payload }], { mode: "evolve" });
    const rows = await db.select("users");
    assert.equal(rows[0].name, payload);
    await assert.rejects(db.insertObjects("bad-name", [{ id: 1 }], { mode: "evolve" }), ValidationError);
  } finally {
    await db.close();
  }
});

it("empty batch, nested json, flatten mode", async () => {
  const db = await createClient({ path: makeDbPath("nesting") });
  try {
    await db.insertObjects("noop_table", [], { mode: "evolve" });
    const exists = await db.query(
      "SELECT COUNT(*)::INTEGER AS c FROM information_schema.tables WHERE table_name = $1",
      ["noop_table"]
    );
    assert.equal(exists[0].c, 0);

    await db.insertObjects(
      "profiles",
      [{ id: 1, profile: { city: "TLV", zip: 12345 }, tags: ["a", "b"] }],
      { mode: "evolve", nested: "json" }
    );
    const rows = await db.select("profiles");
    assert.equal(rows.length, 1);
    assert.ok(rows[0].profile);

    await db.insertObjects("profiles_flat", [{ id: 1, profile: { city: "TLV" } }], {
      mode: "evolve",
      nested: "flatten",
    });
    const flatRows = await db.query('SELECT "id", "profile_city" FROM "profiles_flat"');
    assert.deepEqual(flatRows, [{ id: 1, profile_city: "TLV" }]);
  } finally {
    await db.close();
  }
});

it("file-backed persistence across reopen", async () => {
  const dbPath = makeDbPath("persist");
  {
    const db = await createClient({ path: dbPath });
    await db.insertObjects("sessions", [{ id: 1, token: "abc" }], { mode: "evolve" });
    await db.close();
  }
  {
    const db = await createClient({ path: dbPath });
    const rows = await db.select("sessions");
    assert.deepEqual(rows, [{ id: 1, token: "abc" }]);
    await db.close();
  }
});

it("spatial disabled by default and lazy-enabled path", async () => {
  const disabledDb = await createClient({ path: makeDbPath("spatial-disabled") });
  try {
    assert.equal(disabledDb.spatial.isEnabled(), false);
    const predicate = disabledDb.spatial.whereIntersects("geom", "POINT(0 0)", "wkt");
    await assert.rejects(disabledDb.select("places", {}, { spatial: [predicate] }), SpatialDisabledError);
  } finally {
    await disabledDb.close();
  }

  const enabledDb = await createClient({
    path: makeDbPath("spatial-lazy"),
    spatial: { enabled: true, loadStrategy: "lazy", installIfMissing: false },
  });
  try {
    let ready = false;
    try {
      await enabledDb.spatial.ensureLoaded();
      ready = true;
    } catch {
      ready = false;
    }
    if (ready) {
      await enabledDb.insertObjects(
        "places",
        [{ id: 1, geom: "POINT(0 0)" }],
        {
          mode: "evolve",
          spatialColumns: { geom: { kind: "geometry", input: "wkt" } },
        }
      );
      const rows = await enabledDb.select("places", {}, { spatial: [enabledDb.spatial.whereIntersects("geom", "POINT(0 0)")] });
      assert.equal(rows.length, 1);
    }
  } finally {
    await enabledDb.close();
  }
});

let failures = 0;
for (const { name, fn } of tests) {
  try {
    await fn();
    console.log(`PASS: ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL: ${name}`);
    console.error(error);
  }
}

await rm(TMP_DIR, { recursive: true, force: true });

if (failures > 0) {
  console.error(`\n${failures} test(s) failed.`);
  process.exit(1);
}

console.log(`\nAll ${tests.length} tests passed.`);
