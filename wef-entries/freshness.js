"use strict";
// Admission deadlines are not freshness leases. Publish accepted observations
// at original source time. Absence retirement requires separate policy approval.
function evaluate(item,input){
 const affected=(input.attempts||[]).filter(attempt=>{
  const reserved=attempt.reservedClass?.['class-keys'];
  return Array.isArray(reserved)&&reserved.includes(item.scopeKey);
 });
 return{accepted:true,canRetire:false,current:false,affectedAttempts:affected,
  reason:affected.length?'Accepted observation retained; newer unfinished exact-class attempt noted':'Accepted source observation; currentness is not asserted'};
}
module.exports={evaluate,activationPolicyConfirmed:true};
