import { ValidationError } from "./errors.js";

const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function assertIdentifier(value: string, label: string): void {
  if (!IDENTIFIER_RE.test(value)) {
    throw new ValidationError(`Invalid ${label} identifier: "${value}"`);
  }
}

export function quoteIdentifier(identifier: string): string {
  assertIdentifier(identifier, "SQL");
  return `"${identifier}"`;
}

export function quoteTable(table: string): string {
  assertIdentifier(table, "table");
  return quoteIdentifier(table);
}

export function placeholder(position: number): string {
  return `$${position}`;
}

export function asStringLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}
