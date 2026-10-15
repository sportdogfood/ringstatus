// The same fields are used by the native editor and direct-save boundary.
import {InputError} from './rs-inputs.js';
export const PERSON_FIELDS=['displayName','sms','pin','profileImg','roles','tags','membershipStatus'];
export function profileFields(draft){
 const result={};
 for(const key of [...PERSON_FIELDS,'logo'])if(Object.hasOwn(draft,key)){
  if(typeof draft[key]!=='string'||draft[key].length>2048)throw new InputError('invalid_profile_field',400);
  result[key]=draft[key].trim();
 }
 if(Object.hasOwn(result,'membershipStatus')&&!['Enabled','Disabled'].includes(result.membershipStatus))throw new InputError('invalid_membership_status',400);
 if(result.sms){const digits=result.sms.replace(/\D/g,'');if(!/^(1)?\d{10}$/.test(digits))throw new InputError('invalid_sms',400);result.sms='+'+(digits.length===10?'1':'')+digits;}
 if(result.pin&&!/^\d{4}$/.test(result.pin))throw new InputError('invalid_pin',400);
 for(const key of ['profileImg','logo'])if(result[key]){let url;try{url=new URL(result[key])}catch{}if(!url||url.protocol!=='https:'||url.username||url.password)throw new InputError('invalid_image_url',400);}
 for(const key of ['roles','tags'])if(Object.hasOwn(result,key))result[key]=[...new Set(result[key].split(',').map(x=>x.trim()).filter(Boolean))].sort().join(',');
 return result;
}
