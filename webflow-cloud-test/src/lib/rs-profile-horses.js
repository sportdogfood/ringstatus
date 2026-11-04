import {BASE,TABLE} from './rs-tenant-barn.js';
import {InputError} from './rs-inputs.js';

export const HORSES_TABLE='tblROAbIzrkNMRGVd';
const fields=['horse-id','horse-name','horse-nickname','w-trainers'];
const trainerIds=value=>String(value||'').split(/[,;\s]+/).filter(Boolean);

export async function loadProfileHorses(c){
 const own=c.memberships.filter(m=>m.fields.Person?.includes(c.person.id));
 // Use the same single enabled Team boundary as Profile; never accept a browser trainer selector.
 if(own.length!==1||own[0].fields.Person?.length!==1||own[0].fields.Tenant?.length!==1||own[0].fields.Status!=='Enabled')return {horses:[],horseTeamId:null};
 const team=await c.call(TABLE.barn,{id:own[0].fields.Tenant[0]});
 if(team.fields.tenant_active!==true)return {horses:[],horseTeamId:null};
 const trainer=String(team.fields.tenant_id??'');
 if(!/^\d+$/.test(trainer))throw new InputError('horse_team_reference_required',409);
 const url=new URL(`https://api.airtable.com/v0/${BASE}/${HORSES_TABLE}`);
 url.searchParams.set('filterByFormula',`FIND('${trainer}',{w-trainers})`);
 for(const field of fields)url.searchParams.append('fields[]',field);
 const horses=[],seen=new Set(),callFetch=c.callFetch;
 do{
  const response=await callFetch(url,{headers:{Authorization:`Bearer ${c.env.AIRTABLE_TOKEN}`},redirect:'manual',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw new InputError('horse_storage_unavailable',503);
  const body=await response.json();
  if(!Array.isArray(body.records))throw new InputError('horse_storage_unavailable',502);
  for(const row of body.records){const f=row.fields||{};if(!trainerIds(f['w-trainers']).includes(trainer))continue;
   horses.push({id:row.id,tenant_id:team.id,horseId:f['horse-id'],name:f['horse-name']||'',nickname:f['horse-nickname']||'',source:'w-horses'});
  }
  if(!body.offset)break;
  if(seen.has(body.offset))throw new InputError('horse_storage_unavailable',502);
  seen.add(body.offset);url.searchParams.set('offset',body.offset);
 }while(true);
 horses.sort((a,b)=>a.id.localeCompare(b.id));
 return {horses,horseTeamId:team.id};
}
