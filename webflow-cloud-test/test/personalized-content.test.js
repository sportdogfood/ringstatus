import assert from "node:assert/strict";
import test from "node:test";

import {
  buildPersonalizedSection,
  normalizeContext,
  resolveDatasetUrl
} from "../src/lib/personalized-content.js";

test("filters personalized activities by season and user tags", () => {
  const dataset = {
    seasons: {
      fall: {
        label: "Fall",
        activities: [
          { title: "Trail running", tags: ["running", "fall"] },
          { title: "Ridge hikes", tags: ["hiking", "fall"] },
          { title: "Pool laps", tags: ["swimming", "summer"] }
        ]
      }
    }
  };

  const section = buildPersonalizedSection(dataset, {
    season: "Fall",
    tags: ["running", "hiking", "fall"]
  });

  assert.equal(section.ok, true);
  assert.equal(section.season.label, "Fall");
  assert.deepEqual(section.activities.map((item) => item.title), ["Trail running", "Ridge hikes"]);
  assert.equal(section.copy.headline, "I love when its Fall season");
  assert.equal(section.copy.intro, "In the Fall I like to do these activities");
});

test("normalizes comma-delimited context and returns fallback for no matches", () => {
  const context = normalizeContext({
    season: "winter",
    tags: "running, hiking"
  });

  const section = buildPersonalizedSection({
    seasons: {
      winter: {
        label: "Winter",
        activities: [{ title: "Barn chores", tags: ["horses", "winter"] }]
      }
    }
  }, context);

  assert.deepEqual(context.tags, ["running", "hiking"]);
  assert.equal(section.activities.length, 0);
  assert.equal(section.fallback, "No activities matched this profile yet.");
});

test("resolves dataset URL to fallback when endpoint receives relative browser defaults", () => {
  const fallback = "https://cdn.example.com/personalized-section-content.json";

  assert.equal(resolveDatasetUrl("", fallback), fallback);
  assert.equal(resolveDatasetUrl("./personalized-section-content.json", fallback), fallback);
  assert.equal(resolveDatasetUrl("file:///tmp/personalized-section-content.json", fallback), fallback);
  assert.equal(
    resolveDatasetUrl("https://cdn.example.com/custom.json", fallback),
    "https://cdn.example.com/custom.json"
  );
});
