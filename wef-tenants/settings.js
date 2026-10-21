"use strict";
const path=require('node:path'),schema=require('./schema.json');
const settings={BASE_ID:schema.baseId,SHARED_BASE_ID:schema.sharedBaseId,SHARED_SHOWS_TABLE:'tblBq3gXC0TYBCmMZ',RUNTIME:path.join(__dirname,'runtime')};
for(const key of ['SystemRoot','WINDIR','ComSpec','PATH','PATHEXT','TEMP','TMP','USERPROFILE','APPDATA','LOCALAPPDATA'])if(process.env[key])settings[key]=process.env[key];
for(const key of ['AIRTABLE_TOKEN','SGL_DATA_BASE_URL','SGL_DIRECT_BASE_URL','SGL_FETCH_TRANSPORT','SGL_USER_AGENT','SGL_COOKIE_HEADER','SGL_SEC_CH_UA','SGL_SEC_CH_UA_MOBILE','SGL_SEC_CH_UA_PLATFORM','SGL_RECAPTCHA_TOKEN','SGL_X_RECAPTCHA_TOKEN','SGL_AUTHORIZATION','SGL_BEARER_TOKEN','SGL_FETCH_TIMEOUT_SEC','HTTP_TIMEOUT_MS'])if(process.env['WEF_TENANTS_'+key])settings[key]=process.env['WEF_TENANTS_'+key];
settings.SGL_PAYLOAD_ROOT=settings.RUNTIME;
settings.SGL_FETCH_SCRIPT=path.join(__dirname,'sgl_fetch.ps1');settings.SGL_FETCH_SESSION_JSON=path.join(settings.RUNTIME,'session.json');
module.exports=Object.freeze(settings);
