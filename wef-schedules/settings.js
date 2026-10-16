"use strict";
// Only namespaced workflow settings are accepted. No legacy environment fallback.
const path=require('node:path');
const root=path.join(__dirname,'runtime');
const settings={};
// Essential OS environment for the copied PowerShell transport only.
for(const key of ['SystemRoot','WINDIR','ComSpec','PATH','PATHEXT','TEMP','TMP','USERPROFILE','APPDATA','LOCALAPPDATA'])
  if(process.env[key]) settings[key]=process.env[key];
for(const key of ['AIRTABLE_TOKEN','SGL_DATA_BASE_URL','SGL_DIRECT_BASE_URL','SGL_FETCH_TRANSPORT','SGL_USER_AGENT','SGL_COOKIE_HEADER','SGL_SEC_CH_UA','SGL_SEC_CH_UA_MOBILE','SGL_SEC_CH_UA_PLATFORM','SGL_RECAPTCHA_TOKEN','SGL_X_RECAPTCHA_TOKEN','SGL_AUTHORIZATION','SGL_BEARER_TOKEN','SGL_FETCH_TIMEOUT_SEC','HTTP_TIMEOUT_MS'])
  if(process.env['WEF_SCHEDULES_'+key]) settings[key]=process.env['WEF_SCHEDULES_'+key];
Object.assign(settings,{
  CUSTOMER_ID:'15',AIRTABLE_BASE_ID:'',SCHEDULE_STAGING_BASE_ID:'appUq5Qz9kARqFija',
  SCHEDULE_COLLECTIONS_TABLE:'collections',SCHEDULE_STAGING_TABLE:'schedules',
  SCHEDULE_FOCUS_FILE:path.join(root,'focus.json'),SCHEDULE_COLLECTION_LOCK:path.join(root,'collector.lock'),
  SGL_PAYLOAD_ROOT:root,EARLY_SGL_PAYLOAD_ROOT:path.join(root,'cache'),
  EARLY_SCHEDULE_PAYLOAD_DIR:path.join(root,'cache','schedule'),
  MANUAL_SGL_PAYLOAD_ROOT:path.join(root,'manual'),MANUAL_SCHEDULE_PAYLOAD_DIR:path.join(root,'manual','schedule'),
  MANUAL_SCHEDULE_HTML_DIR:path.join(root,'manual','schedule-html'),
  SGL_FETCH_SCRIPT:path.join(__dirname,'sgl_fetch.ps1'),SGL_FETCH_SESSION_JSON:path.join(root,'session.json'),
  PREFETCH_FORWARD_SCHEDULES:'0',DRY_RUN:'0'
});
module.exports=Object.freeze(settings);
