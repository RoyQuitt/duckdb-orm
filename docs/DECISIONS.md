# Decisions Log

This file records product and engineering decisions we make during development.

## 2026-04-26

### Documentation location
- Decision: Project planning/working Markdown files will live under `docs/`.
- Rationale: Keep repository root clean and make docs easier to find.

### Root README convention
- Decision: `README.md` stays at repository root.
- Rationale: Standard package/repo convention for GitHub and npm discovery.

### Spatial support default
- Decision: Spatial extension support is toggleable at client init and disabled by default (`spatial.enabled = false`).
- Rationale: Avoid extension load/install overhead unless teams explicitly opt in.

### Demo client location and format
- Decision: Provide a runnable demo client at `examples/demo-client.mjs`, executed via `npm run demo`.
- Rationale: Keep usage examples out of production `src/` while making onboarding/test-driving the library fast.

### Demo spatial installation behavior
- Decision: Demo client should attempt auto-install for spatial (`installIfMissing: true`).
- Rationale: Demonstrate the full opt-in spatial workflow by default in the example script.
