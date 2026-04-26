import { mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createClient } from "../../src/client.js";
import { SchemaConflictError, SpatialDisabledError, ValidationError } from "../../src/errors.js";

const TMP_DIR = path.resolve(process.cwd(), "tests", ".tmp");

beforeAll(async () => {
  await mkdir(TMP_DIR, { recursive: true });
});

afterAll(async () => {
  await rm(TMP_DIR, { recursive: true, force: true });
});

function makeDbPath(name: string): string {
  return path.join(TMP_DIR, `${name}-${randomUUID()}.duckdb`);
}

async function withClient<T>(
  name: string,
  fn: (db: Awaited<ReturnType<typeof createClient>>, dbPath: string) => Promise<T>
): Promise<T> {
  const dbPath = makeDbPath(name);
  const db = await createClient({ path: dbPath });
  try {
    return await fn(db, dbPath);
  } finally {
    await db.close();
  }
}

describe("client integration", () => {
  test("connects and runs basic query", async () => {
    await withClient("basic-query", async (db) => {
      const rows = await db.query<{ value: number }>("SELECT 42::INTEGER AS value");
      expect(rows).toEqual([{ value: 42 }]);
    });
  });

  test("auto derives schema, inserts and selects", async () => {
    await withClient("derive-insert-select", async (db) => {
      await db.insertObjects("users", [{ id: 1, name: "Ada", active: true }], { mode: "evolve" });
      const users = await db.select<{ id: number; name: string; active: boolean }>("users", { id: 1 });
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe("Ada");
      expect(users[0].active).toBe(true);
    });
  });

  test("evolve mode adds missing columns", async () => {
    await withClient("schema-evolve", async (db) => {
      await db.insertObjects("events", [{ id: 1 }], { mode: "evolve" });
      const result = await db.ensureSchema([{ id: 1, title: "launch" }], { table: "events", mode: "evolve" });
      expect(result.alteredColumns).toContain("title");

      await db.insertObjects("events", [{ id: 2, title: "launch" }], { mode: "evolve" });
      const rows = await db.select<{ id: number; title: string | null }>("events");
      expect(rows).toHaveLength(2);
      expect(rows.some((row) => row.title === "launch")).toBe(true);
    });
  });

  test("strict mode rejects schema drift", async () => {
    await withClient("schema-strict", async (db) => {
      await db.insertObjects("metrics", [{ id: 1 }], { mode: "evolve" });
      await expect(
        db.ensureSchema([{ id: 1, name: "x" }], {
          table: "metrics",
          mode: "strict",
        })
      ).rejects.toThrow(SchemaConflictError);
    });
  });

  test("rejects mixed-shape batch insert", async () => {
    await withClient("mixed-shape", async (db) => {
      await expect(
        db.insertObjects(
          "logs",
          [
            { id: 1, message: "a" },
            { id: 2 },
          ],
          { mode: "evolve" }
        )
      ).rejects.toThrow(ValidationError);
    });
  });

  test("transaction commit persists and rollback reverts", async () => {
    await withClient("transaction", async (db) => {
      await db.ensureSchema([{ id: 1, label: "x" }], { table: "tx_rows", mode: "evolve" });

      await db.transaction(async (tx) => {
        await tx.insertObjects("tx_rows", [{ id: 1, label: "ok" }], { mode: "evolve" });
      });
      let rows = await db.select<{ id: number; label: string }>("tx_rows");
      expect(rows).toHaveLength(1);

      await expect(
        db.transaction(async (tx) => {
          await tx.insertObjects("tx_rows", [{ id: 2, label: "rollback" }], { mode: "evolve" });
          throw new Error("force rollback");
        })
      ).rejects.toThrow("force rollback");

      rows = await db.select<{ id: number; label: string }>("tx_rows");
      expect(rows).toHaveLength(1);
      expect(rows[0].label).toBe("ok");
    });
  });

  test("value parameterization avoids sql injection from inserted values", async () => {
    await withClient("injection", async (db) => {
      const payload = "x'); DROP TABLE users; --";
      await db.insertObjects("users", [{ id: 1, name: payload }], { mode: "evolve" });
      const rows = await db.select<{ name: string }>("users");
      expect(rows).toHaveLength(1);
      expect(rows[0].name).toBe(payload);
    });
  });

  test("invalid table identifiers are rejected", async () => {
    await withClient("invalid-identifier", async (db) => {
      await expect(db.insertObjects("bad-name", [{ id: 1 }], { mode: "evolve" })).rejects.toThrow(ValidationError);
    });
  });

  test("empty batch insert is a no-op", async () => {
    await withClient("empty-batch", async (db) => {
      await db.insertObjects("noop_table", [], { mode: "evolve" });
      const exists = await db.query<{ c: number }>(
        "SELECT COUNT(*)::INTEGER AS c FROM information_schema.tables WHERE table_name = $1",
        ["noop_table"]
      );
      expect(exists[0].c).toBe(0);
    });
  });

  test("nested json mode stores nested values", async () => {
    await withClient("nested-json", async (db) => {
      await db.insertObjects(
        "profiles",
        [{ id: 1, profile: { city: "TLV", zip: 12345 }, tags: ["a", "b"] }],
        { mode: "evolve", nested: "json" }
      );
      const rows = await db.select<{ id: number; profile: unknown; tags: unknown }>("profiles");
      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe(1);
      expect(rows[0].profile).toBeTruthy();
      expect(rows[0].tags).toBeTruthy();
    });
  });

  test("flatten mode creates flattened columns", async () => {
    await withClient("nested-flatten", async (db) => {
      await db.insertObjects("profiles_flat", [{ id: 1, profile: { city: "TLV" } }], {
        mode: "evolve",
        nested: "flatten",
      });
      const rows = await db.query<{ id: number; profile_city: string }>(
        'SELECT "id", "profile_city" FROM "profiles_flat"'
      );
      expect(rows).toEqual([{ id: 1, profile_city: "TLV" }]);
    });
  });

  test("file-backed db persists across reopen", async () => {
    const dbPath = makeDbPath("persist");
    {
      const db = await createClient({ path: dbPath });
      await db.insertObjects("sessions", [{ id: 1, token: "abc" }], { mode: "evolve" });
      await db.close();
    }
    {
      const db = await createClient({ path: dbPath });
      const rows = await db.select<{ id: number; token: string }>("sessions");
      expect(rows).toEqual([{ id: 1, token: "abc" }]);
      await db.close();
    }
  });

  test("spatial is disabled by default and spatial usage fails clearly", async () => {
    await withClient("spatial-disabled", async (db) => {
      expect(db.spatial.isEnabled()).toBe(false);
      const predicate = db.spatial.whereIntersects("geom", "POINT(0 0)", "wkt");
      await expect(db.select("places", {}, { spatial: [predicate] })).rejects.toThrow(SpatialDisabledError);
    });
  });
});
