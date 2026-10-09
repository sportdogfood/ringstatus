// Read-only schema evidence captured 2026-10-08 in app9kOZdIaGyKk5uG.
// Extend the older fixture config in memory; do not provision or modify schema.
export function withInputLifecycle(schema) {
  const result=structuredClone(schema);
  for(const table of result.tables) {
    if(!['rs_input_barns','rs_input_users','rs_input_riders','rs_input_horses','rs_input_locations','rs_input_events'].includes(table.name)) continue;
    if(table.name!=='rs_input_events') table.fields.push({name:'status',type:'singleSelect',options:{choices:[{name:'Active'},{name:'Inactive'},{name:'Archived'}]}});
    table.fields.push({name:'record_mode',type:'singleSelect',options:{choices:[{name:'Test'},{name:'Live'}]}});
  }
  return result;
}
