"use strict";
const path = require("node:path");
module.exports = Object.freeze({
  BASE_ID: "appgzosUcVqA4vZdf",
  SCHEDULES_BASE: "appUq5Qz9kARqFija",
  ENTRIES_BASE: "appSxF4pYMBaxpTZd",
  RUNTIME: path.join(__dirname, "runtime"),
  AIRTABLE_TOKEN: process.env.WEF_LIVE_AIRTABLE_TOKEN,
});
