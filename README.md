# duck-db-orm

TypeScript library for DuckDB with:
- automatic schema derivation/evolution
- object-shape agnostic inserts
- basic select filtering
- optional spatial extension support (disabled by default)

## Install

```bash
npm install duck-db-orm @duckdb/node-api
```

## Quick Start

```ts
import { createClient } from "duck-db-orm";

const db = await createClient({
  path: "app.duckdb",
  spatial: {
    enabled: false, // default
  },
});

await db.insertObjects("users", [{ id: 1, name: "Ada", active: true }], {
  mode: "evolve",
});

const users = await db.select("users", { active: true });
await db.close();
```

## Spatial (Opt-In)

```ts
const db = await createClient({
  spatial: {
    enabled: true,
    installIfMissing: true,
    loadStrategy: "onInit", // or "lazy"
  },
});

await db.insertObjects(
  "places",
  [{ id: 1, geom: "POINT(34.78 32.08)" }],
  {
    mode: "evolve",
    spatialColumns: {
      geom: { kind: "geometry", input: "wkt" },
    },
  }
);

const nearby = await db.select(
  "places",
  {},
  {
    spatial: [db.spatial.whereDWithin("geom", "POINT(34.78 32.08)", 0.05, "wkt")],
    geometryColumns: ["geom"],
    geometryOutput: "wkt",
  }
);
```
