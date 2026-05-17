const assert = require("assert");
const fs = require("fs");
const path = require("path");

const source = fs.readFileSync(
  path.resolve(__dirname, "..", "schedules_calculatorv2.js"),
  "utf8"
);

assert.ok(
  source.includes("TABLE_WW_PROFILES"),
  "class_tills must fetch ww_profiles directly for profile subscriptions"
);

assert.ok(
  source.includes("fetchClassTillsProfiles"),
  "class_tills must load subscribed profiles before creating threads"
);

assert.ok(
  source.includes("profileMilestoneValue"),
  "class_tills milestones must come from the profile record"
);

assert.ok(
  source.includes("isProfilewise"),
  "class_tills must distinguish profilewise overrides from default tenant/profile alerts"
);

assert.ok(
  source.includes("alertPriority"),
  "class_tills must rank profilewise override rows ahead of default rows"
);

assert.ok(
  source.includes("alertMilestoneValue"),
  "class_tills default records must carry a direct default milestone value"
);

assert.ok(
  source.includes("buildProfilewiseClassTillsConfigs"),
  "class_tills must build profilewise configs only from explicitly linked profiles"
);

assert.ok(
  source.includes("buildDefaultClassTillsConfigs"),
  "class_tills must build default configs from eligible subscribed profiles"
);

assert.ok(
  !source.includes("alert_milestone1 (from ww_tenants)") &&
    !source.includes("alert_milestone2 (from ww_tenants)"),
  "class_tills must not request removed active_alerts rollup field names"
);

assert.ok(
  source.includes("activeSubscriberIds"),
  "class_tills must require active subscriber links on the profile"
);

assert.ok(
  source.includes("qualifiedTripIds"),
  "class_tills must create threads only from profile-qualified trips"
);

assert.ok(
  source.includes('"ww_profiles"') && source.includes('"active_subscribers"'),
  "thread_logs must link the subscribed profile and active subscribers"
);

assert.ok(
  source.includes("tenant_profile_key"),
  "profile-specific thread identity must include a stable tenant profile key"
);

console.log("schedules_calculator_class_tills_profile_gate tests passed");
