export class DuckOrmError extends Error {
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = "DuckOrmError";
  }
}

export class ValidationError extends DuckOrmError {
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

export class SchemaConflictError extends DuckOrmError {
  constructor(message: string) {
    super(message);
    this.name = "SchemaConflictError";
  }
}

export class SpatialDisabledError extends DuckOrmError {
  constructor() {
    super("Spatial extension is disabled. Enable it in client init options: spatial.enabled = true.");
    this.name = "SpatialDisabledError";
  }
}

export class QueryExecutionError extends DuckOrmError {
  constructor(message: string, cause?: unknown) {
    super(message, cause);
    this.name = "QueryExecutionError";
  }
}
