const RULES_TABLE = "group_tags";

const RULE_FIELDS = [
  "name_tag",
  "is_active",
  "source_table",
  "source_view",
  "target_field",
  "target_field_type",
  "tag_class",
  "output_field",
  "match_type",
  "priority",
  "match_pattern",
  "watch_schedule",
  "watch_trips"
];

const SOURCE_LINK_FIELD = "group_tags_links";

function asString(v) {
  return String(v ?? "").trim();
}

function normText(v) {
  return String(v ?? "")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function normFold(v) {
  return normText(v).toLowerCase();
}

function toBool(v) {
  if (v === true || v === false) return v;
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "checked";
}

function getCellString(record, fieldName) {
  return asString(record.getCellValueAsString(fieldName));
}

function getCheckboxValue(record, fieldName) {
  const raw = record.getCellValue(fieldName);
  return raw === true;
}

function escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function compileRule(rule) {
  const matchType = rule.matchType;
  const pattern = rule.matchPattern || "";

  if (matchType === "regex") {
    try {
      rule.regex = new RegExp(pattern, "i");
    } catch (err) {
      rule.invalid = `Invalid regex: ${pattern}`;
    }
  } else if (matchType === "contains") {
    rule.containsValue = normFold(pattern);
  } else if (matchType === "exact") {
    rule.exactValue = normFold(pattern);
  }
  return rule;
}

function ruleMatches(rule, sourceRecord) {
  const fieldName = rule.targetField;
  const fieldType = rule.targetFieldType;
  const matchType = rule.matchType;

  if (fieldType === "checkbox") {
    const checked = getCheckboxValue(sourceRecord, fieldName);
    if (matchType === "checkbox_true") return checked === true;
    if (matchType === "checkbox_false") return checked === false;

    const textVal = checked ? "true" : "false";
    if (matchType === "contains") return textVal.includes(rule.containsValue);
    if (matchType === "exact") return textVal === rule.exactValue;
    if (matchType === "regex") return rule.regex ? rule.regex.test(textVal) : false;
    return false;
  }

  const rawText = getCellString(sourceRecord, fieldName);
  const text = normText(rawText);
  const fold = text.toLowerCase();

  if (!text) return false;

  if (matchType === "contains") return fold.includes(rule.containsValue);
  if (matchType === "exact") return fold === rule.exactValue;
  if (matchType === "regex") return rule.regex ? rule.regex.test(text) : false;
  if (matchType === "checkbox_true") return false;
  if (matchType === "checkbox_false") return false;

  return false;
}

function linkedIdsEqual(existingLinks, newIds) {
  const a = (existingLinks || []).map(x => x.id).sort();
  const b = [...newIds].sort();
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function updateInBatches(table, updates) {
  for (let i = 0; i < updates.length; i += 50) {
    await table.updateRecordsAsync(updates.slice(i, i + 50));
  }
}

async function main() {
  const rulesTable = base.getTable(RULES_TABLE);
  const rulesQuery = await rulesTable.selectRecordsAsync({ fields: RULE_FIELDS });

  const allRules = rulesQuery.records
    .filter(r => toBool(r.getCellValue("is_active")))
    .map(r => compileRule({
      recordId: r.id,
      nameTag: getCellString(r, "name_tag"),
      sourceTable: getCellString(r, "source_table"),
      sourceView: getCellString(r, "source_view"),
      targetField: getCellString(r, "target_field"),
      targetFieldType: getCellString(r, "target_field_type").toLowerCase(),
      tagClass: getCellString(r, "tag_class"),
      outputField: getCellString(r, "output_field"),
      matchType: getCellString(r, "match_type").toLowerCase(),
      priority: Number(r.getCellValue("priority") || 999999),
      matchPattern: getCellString(r, "match_pattern")
    }))
    .filter(r =>
      r.nameTag &&
      r.sourceTable &&
      r.targetField &&
      r.targetFieldType &&
      r.outputField &&
      r.matchType
    )
    .filter(r => !r.invalid);

  const grouped = new Map();

  for (const rule of allRules) {
    const key = `${rule.sourceTable}|||${rule.sourceView}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(rule);
  }

  let processedRuleGroups = 0;
  let processedSourceRecords = 0;
  let updatedSourceRecords = 0;
  let updatedRuleLinks = 0;

  for (const [groupKey, rules] of grouped.entries()) {
    const [sourceTableName, sourceViewName] = groupKey.split("|||");
    const sourceTable = base.getTable(sourceTableName);
    const sourceView = sourceTable.getView(sourceViewName);

    const neededSourceFields = [...new Set([
      SOURCE_LINK_FIELD,
      ...rules.map(r => r.targetField),
      ...rules.map(r => r.outputField)
    ])];

    const sourceQuery = await sourceView.selectRecordsAsync({ fields: neededSourceFields });

    const sourceUpdates = [];
    const ruleBacklinkAdds = new Map();

    const sortedRules = [...rules].sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.nameTag.localeCompare(b.nameTag);
    });

    for (const sourceRecord of sourceQuery.records) {
      processedSourceRecords++;

      const matched = [];
      const matchedRuleIds = [];
      const seenTags = new Set();

      for (const rule of sortedRules) {
        let didMatch = false;
        try {
          didMatch = ruleMatches(rule, sourceRecord);
        } catch (err) {
          didMatch = false;
        }

        if (!didMatch) continue;

        const tagKey = rule.nameTag.toLowerCase();
        if (!seenTags.has(tagKey)) {
          seenTags.add(tagKey);
          matched.push({
            nameTag: rule.nameTag,
            priority: rule.priority
          });
        }

        matchedRuleIds.push(rule.recordId);

        if (!ruleBacklinkAdds.has(rule.recordId)) {
          ruleBacklinkAdds.set(rule.recordId, new Set());
        }
        ruleBacklinkAdds.get(rule.recordId).add(sourceRecord.id);
      }

      const tagsCsv = matched
        .sort((a, b) => {
          if (a.priority !== b.priority) return a.priority - b.priority;
          return a.nameTag.localeCompare(b.nameTag);
        })
        .map(x => x.nameTag)
        .join(", ");

      const outputField = sortedRules[0].outputField;
      const existingCsv = getCellString(sourceRecord, outputField);
      const existingLinks = sourceRecord.getCellValue(SOURCE_LINK_FIELD) || [];
      const desiredLinkIds = [...new Set(matchedRuleIds)];

      const csvChanged = existingCsv !== tagsCsv;
      const linksChanged = !linkedIdsEqual(existingLinks, desiredLinkIds);

      if (csvChanged || linksChanged) {
        const fields = {};
        fields[outputField] = tagsCsv;
        fields[SOURCE_LINK_FIELD] = desiredLinkIds.map(id => ({ id }));

        sourceUpdates.push({
          id: sourceRecord.id,
          fields
        });
      }
    }

    if (sourceUpdates.length) {
      await updateInBatches(sourceTable, sourceUpdates);
      updatedSourceRecords += sourceUpdates.length;
    }

    const backlinkFieldName =
      sourceTableName === "watch_schedule" ? "watch_schedule" :
      sourceTableName === "watch_trips" ? "watch_trips" :
      null;

    if (backlinkFieldName) {
      const ruleLinkUpdates = [];

      for (const ruleRecord of rulesQuery.records) {
        const adds = ruleBacklinkAdds.get(ruleRecord.id);
        if (!adds || adds.size === 0) continue;

        const existing = ruleRecord.getCellValue(backlinkFieldName) || [];
        const merged = new Map();

        for (const link of existing) merged.set(link.id, { id: link.id });
        for (const id of adds) merged.set(id, { id });

        const mergedIds = [...merged.keys()];
        const existingIds = existing.map(x => x.id);

        const same =
          mergedIds.length === existingIds.length &&
          mergedIds.slice().sort().every((id, idx) => id === existingIds.slice().sort()[idx]);

        if (!same) {
          ruleLinkUpdates.push({
            id: ruleRecord.id,
            fields: {
              [backlinkFieldName]: [...merged.values()]
            }
          });
        }
      }

      if (ruleLinkUpdates.length) {
        await updateInBatches(rulesTable, ruleLinkUpdates);
        updatedRuleLinks += ruleLinkUpdates.length;
      }
    }

    processedRuleGroups++;
  }

  output.set("processed_rule_groups", processedRuleGroups);
  output.set("processed_source_records", processedSourceRecords);
  output.set("updated_source_records", updatedSourceRecords);
  output.set("updated_rule_links", updatedRuleLinks);
  output.set("active_rules", allRules.length);
}

await main();
