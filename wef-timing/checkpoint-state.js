'use strict';
const zlib=require('node:zlib'),crypto=require('node:crypto');
const digest=s=>crypto.createHash('sha256').update(s).digest('hex');
function decode(text){const x=JSON.parse(text);if(x.encoding!=='ring-state-gzip-v1')return x;const raw=zlib.gunzipSync(Buffer.from(x.data,'base64'),{maxOutputLength:16*1024*1024}).toString('utf8');if(digest(raw)!==x.sha256)throw Error('Checkpoint checksum mismatch');return JSON.parse(raw);}
function encode(state){const raw=JSON.stringify(state);const body=JSON.stringify({encoding:'ring-state-gzip-v1',sha256:digest(raw),data:zlib.gzipSync(raw).toString('base64')});if(body.length>95000)throw Error('Checkpoint too large; no state discarded');return body;}
module.exports={decode,encode};
