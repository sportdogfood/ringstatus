"use strict";
const path=require('node:path');
const settings={BASE_ID:'appCaSmL819KK7UFJ',RUNTIME:path.join(__dirname,'runtime')};
for(const k of ['SystemRoot','WINDIR','ComSpec','PATH','PATHEXT','TEMP','TMP','USERPROFILE','APPDATA','LOCALAPPDATA'])if(process.env[k])settings[k]=process.env[k];
for(const k of ['AIRTABLE_TOKEN','SGL_FETCH_TRANSPORT','SGL_USER_AGENT','SGL_COOKIE_HEADER','SGL_AUTHORIZATION','SGL_FETCH_TIMEOUT_SEC','HTTP_TIMEOUT_MS'])if(process.env['WEF_SHOWS_'+k])settings[k]=process.env['WEF_SHOWS_'+k];
settings.SGL_PAYLOAD_ROOT=settings.RUNTIME;
settings.SGL_FETCH_SCRIPT=path.join(__dirname,'../wef-schedules/sgl_fetch.ps1');
settings.SGL_FETCH_SESSION_JSON=path.join(settings.RUNTIME,'session.json');
module.exports=Object.freeze(settings);
