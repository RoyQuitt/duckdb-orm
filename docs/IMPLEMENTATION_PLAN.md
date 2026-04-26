# DuckDB ORM Library Plan (TypeScript)

## Scope (Current)
- Build a TypeScript library that wraps DuckDB interactions.
- Support automatic schema derivation from inserted objects.
- Support regular SQL filtering and spatial/geographical queries via DuckDB `spatial` extension.
- Start with core operations: connection, insert, select, and transaction-safe behavior.

## Core Principles
- SQL-first behavior with safe parameter binding.
- Object-shape agnostic API (works with unknown object structures).
- Deterministic schema evolution rules.
- Explicit extension lifecycle management for `spatial`.

## High-Level Architecture
- `src/client/`
  - Connection lifecycle, instance cache usage, transaction wrapper.
- `src/schema/`
  - Schema inference, table creation, schema evolution and conflict detection.
- `src/query/`
  - SQL builders for insert/select/filter, identifier safety, parameter binding.
- `src/spatial/`
  - Spatial extension bootstrap and spatial helper predicates.
- `src/types/`
  - Public TypeScript interfaces and options.
- `src/errors/`
  - Stable library-level error types and messages.

## Public API (v0 Plan)
- `createClient(options): Promise<DuckOrm>`
- `ensureSchema(sample, options): Promise<ResolvedSchema>`
- `insertObjects(table, rows, options?): Promise<void>`
- `upsertObjects(table, rows, options?): Promise<void>` (optional in first cut)
- `select(table, where?, options?): Promise<T[]>`
- `exec(sql, params?): Promise<void>`
- `query(sql, params?): Promise<T[]>`
- `transaction(fn): Promise<R>`
- `close(): Promise<void>`

### Client Init Options (v0 Plan)
- `spatial.enabled?: boolean` (default `false`)
- `spatial.installIfMissing?: boolean` (only relevant when `enabled = true`)
- `spatial.repository?: 'core' | 'core_nightly' | string` (only relevant when install is allowed)
- `spatial.loadStrategy?: 'onInit' | 'lazy'` (default `onInit` when enabled)

### Spatial APIs (v0 Plan)
- `ensureSpatial(options?): Promise<void>`
- `spatial.isReady(): Promise<boolean>`
- `spatial.whereIntersects(column, geometry, inputFormat?): SpatialPredicate`
- `spatial.whereDWithin(column, geometry, distance, inputFormat?): SpatialPredicate`

## Schema Derivation Rules
- Primitive mappings:
  - `string -> VARCHAR`
  - `number -> DOUBLE` (or integer class if stable and configured)
  - `boolean -> BOOLEAN`
  - `bigint -> BIGINT`
  - `Date -> TIMESTAMP`
  - `null/undefined -> nullable`
- Nested data strategy:
  - Default: store nested object/array as `JSON`.
  - Optional mode: flatten nested keys into columns with deterministic naming.
- Naming and safety:
  - Sanitize/quote identifiers.
  - Reject invalid or empty derived schemas.

## Schema Modes
- `strict`
  - Any schema mismatch or unseen fields raises an error.
- `evolve`
  - New fields can be added using `ALTER TABLE ADD COLUMN`.
  - Incompatible type conflicts still fail with explicit diagnostics.

## Spatial Extension Support Plan
- Spatial is opt-in and disabled by default.
- Behavior when `spatial.enabled = false`:
  - Do not run `INSTALL spatial` or `LOAD spatial`.
  - Spatial APIs return a clear "spatial not enabled" error.
- Behavior when `spatial.enabled = true`:
  - `onInit` strategy: attempt `LOAD spatial` during client initialization.
  - `lazy` strategy: attempt `LOAD spatial` on first spatial API call.
  - If missing and allowed, run `INSTALL spatial` then `LOAD spatial`.
- Expose configuration:
  - `enabled` (default `false`)
  - `installIfMissing` (default `false`)
  - `repository` (`core`, `core_nightly`, or custom URL)
  - `loadStrategy` (`onInit` or `lazy`)
- Geometry handling:
  - Support geometry inputs via explicit field metadata:
    - `wkt`, `geojson`, `wkb`
  - Use extension functions for spatial filtering (`ST_Intersects`, `ST_DWithin`, etc.).

## Query Behavior
- Inserts:
  - Always explicit column list.
  - Batch insert for multiple rows of same shape.
- Select:
  - Generic typed row objects.
  - Base equality filters + optional spatial predicates.
- Transactions:
  - `BEGIN` / `COMMIT` / `ROLLBACK` with same connection context.

## Testing Strategy

### Integration Tests
- Connect to `:memory:` and execute basic query.
- Auto-derive schema and create table from first sample object.
- Insert/select roundtrip for primitive fields.
- Schema mode checks:
  - `strict` rejects drift.
  - `evolve` adds columns for new fields.
- Batch insert behavior and mixed-shape validation.
- Transaction commit and rollback behavior.
- File-backed DB reopen persistence.

### Spatial Integration Tests
- Spatial disabled by default:
  - no extension load/install attempted
  - spatial API call fails with clear "disabled" error
- Spatial enabled:
- Spatial bootstrap path:
  - preinstalled extension load
  - install-then-load path (when enabled)
- Geometry insert/select roundtrip.
- Spatial filtering:
  - `ST_Intersects`
  - `ST_DWithin`
- Error path when spatial extension is unavailable and auto-install disabled.

### Unit Tests
- SQL/identifier generation.
- Type inference and merge/conflict logic.
- Value normalization for insert parameters.
- Spatial predicate SQL generation and parameter binding.
- Error wrapping and message quality.

## Risks and Guardrails
- Auto-schema can become unstable if sample data is inconsistent.
  - Mitigation: explicit mode flags + deterministic conflict rules.
- Spatial extension availability differs by environment.
  - Mitigation: explicit startup checks and actionable error messages.
- Type widening can reduce precision.
  - Mitigation: configurable inference policy and explicit field overrides.

## Delivery Phases
1. Core client + `exec/query/transaction`.
2. Auto schema derivation + insert/select for non-spatial data.
3. Spatial bootstrap + spatial predicate support.
4. Hardening: tests, edge cases, and docs/examples.
