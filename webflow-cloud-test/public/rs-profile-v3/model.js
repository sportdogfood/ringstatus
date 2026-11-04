export const TAGS=['trainer','groom','rider','vet','farrier','owner','contact','family'];
const normalizePhone=v=>String(v||'').replace(/\D/g,'');
const fold=value=>String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
export function queryUsers(users,query='',sort='last_name'){
 const terms=fold(query).trim().split(/\s+/).filter(Boolean);
 const compareName=(a,b)=>fold(a.last_name).localeCompare(fold(b.last_name))||fold(a.first_name).localeCompare(fold(b.first_name))||String(a.id).localeCompare(String(b.id));
 const dateValue=v=>Number.isFinite(Date.parse(v))?Date.parse(v):0;
 return users.filter(u=>{const text=fold([u.first_name,u.last_name,u.email,u.sms,normalizePhone(u.sms),u.role,...(u.tags||[])].join(' '));return terms.every(t=>text.includes(t));}).sort((a,b)=>sort==='modified_at'||sort==='created_at'?(dateValue(b[sort])-dateValue(a[sort])||compareName(a,b)):compareName(a,b));
}
