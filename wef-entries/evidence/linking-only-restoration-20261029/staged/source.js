"use strict";
// Source acquisition belongs to Data. No direct endpoint fallback is permitted.
function disabled(){throw Error('Direct Entries acquisition retired; consume accepted Data records');}
module.exports={fetchPeople:disabled,fetchClass:disabled,classEntries:disabled};
