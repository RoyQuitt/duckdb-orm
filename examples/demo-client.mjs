import { createClient } from "../dist/index.js";

async function runCoreDemo() {
  const db = await createClient({
    path: ":memory:",
    spatial: {
      enabled: false,
    },
  });

  try {
    console.log("== Core Demo ==");

    await db.insertObjects(
      "users",
      [
        {
          id: 1,
          name: "Ada",
          isActive: true,
          profile: { city: "Tel Aviv", team: "Data" },
        },
        {
          id: 2,
          name: "Linus",
          isActive: false,
          profile: { city: "Haifa", team: "Infra" },
        },
      ],
      {
        mode: "evolve",
        nested: "json",
      }
    );

    const activeUsers = await db.select("users", { isActive: true });
    console.log("Active users:", activeUsers);

    await db.transaction(async (tx) => {
      await tx.insertObjects(
        "events",
        [
          { id: 101, userId: 1, action: "login" },
          { id: 102, userId: 1, action: "query" },
        ],
        { mode: "evolve" }
      );
    });

    const userEvents = await db.select("events", { userId: 1 }, { limit: 10 });
    console.log("Events for userId=1:", userEvents);
  } finally {
    await db.close();
  }
}

async function runSpatialDemo() {
  const db = await createClient({
    path: ":memory:",
    spatial: {
      enabled: true,
      loadStrategy: "lazy",
      installIfMissing: true,
    },
  });

  try {
    console.log("\n== Spatial Demo (Optional) ==");

    try {
      await db.spatial.ensureLoaded();
    } catch (error) {
      console.log("Spatial extension not available in this environment, skipping spatial demo.");
      console.log(`Reason: ${String(error)}`);
      return;
    }

    await db.insertObjects(
      "places",
      [
        { id: 1, name: "HQ", geom: "POINT(34.7818 32.0853)" },
        { id: 2, name: "Remote", geom: "POINT(34.7900 32.0900)" },
      ],
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
        spatial: [db.spatial.whereDWithin("geom", "POINT(34.7818 32.0853)", 0.02, "wkt")],
        geometryColumns: ["geom"],
        geometryOutput: "wkt",
      }
    );

    console.log("Nearby places:", nearby);
  } finally {
    await db.close();
  }
}

async function main() {
  await runCoreDemo();
  await runSpatialDemo();
}

main().catch((error) => {
  console.error("Demo failed.");
  console.error(error);
  process.exit(1);
});
