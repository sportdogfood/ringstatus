export const RECOGNIZE_BASE_ID = "app9kOZdIaGyKk5uG";
export const RECOGNIZE_TABLES = Object.freeze({
  people: "tbly1PM5iFYqVzKSm", devices: "tblfkRSJAEMzuzApR",
  aliases: "tblgDWKi0Bb6OcoqS", sessions: "tblWjbASVMIjFLyW8"
});

// This check runs before any Airtable access, including logging and recovery.
export function recognitionConfig(env, ErrorType = Error) {
  const fail = (code) => { throw new ErrorType(code, 500); };
  const explicitBase = String(env?.AIRTABLE_RS_RECOGNITION_BASE_ID || "").trim();
  const existingInputBase = String(env?.RS_INPUTS_RECOGNITION_BASE_ID || "").trim();
  // Both are explicit recognition-only bindings. Never inherit AIRTABLE_BASE_ID
  // or the operational-input base, and never tolerate conflicting bindings.
  if (explicitBase && existingInputBase && explicitBase !== existingInputBase) fail("recognition_base_mismatch");
  const baseId = explicitBase || existingInputBase;
  if (!baseId) fail("missing_airtable_base_id");
  if (baseId !== RECOGNIZE_BASE_ID) fail("recognition_base_mismatch");
  const token = String(env?.AIRTABLE_TOKEN || "").trim();
  if (!token) fail("missing_airtable_token");
  const config = { token, baseId };
  for (const [key, variable, name] of [
    ["people", "AIRTABLE_RS_PEOPLE_TEST_TABLE", "rs_people_test"],
    ["devices", "AIRTABLE_RS_DEVICES_TEST_TABLE", "rs_devices_test"],
    ["aliases", "AIRTABLE_RS_PHONE_ALIASES_TEST_TABLE", "rs_phone_aliases_test"],
    ["sessions", "AIRTABLE_RS_RECOGNITION_SESSIONS_TEST_TABLE", "rs_recognition_sessions_test"]
  ]) {
    const configured = String(env?.[variable] || "").trim();
    if (configured && configured !== name && configured !== RECOGNIZE_TABLES[key]) fail("recognition_table_mismatch");
    config[key] = RECOGNIZE_TABLES[key];
  }
  return config;
}
