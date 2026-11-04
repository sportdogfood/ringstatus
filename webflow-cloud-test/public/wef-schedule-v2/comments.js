// Prototype-only comments, stored in this browser; no external messages are sent.
const key='ringstatus-v2-comments-v1';
const seed=[{id:'demo-1',classKey:'r2-825',parentId:null,author:'Demo trainer',text:'Meet at the ring before the class starts.'},{id:'demo-2',classKey:'r2-825',parentId:'demo-1',author:'Demo rider',text:'On our way from the barn.'},{id:'demo-3',classKey:'r2-825',parentId:null,author:'Demo groom',text:'Both horses are ready.'}];
let items=seed;
try{const saved=JSON.parse(globalThis.localStorage?.getItem(key)??'null');if(Array.isArray(saved)&&saved.every(c=>c&&typeof c.id==='string'&&typeof c.classKey==='string'&&typeof c.text==='string'&&typeof c.author==='string'&&(c.parentId===null||typeof c.parentId==='string')))items=saved;}catch{}
export const commentsFor=(classKey,entryKey=null)=>items.filter(c=>c.classKey===classKey&&(c.entryKey??null)===entryKey);
export const commentCount=(classKey,entryKey=null)=>commentsFor(classKey,entryKey).length;
export function addComment(classKey,text,parentId=null){text=text.trim();if(!text||text.length>1000)return false;if(parentId&&!items.some(c=>c.id===parentId&&c.classKey===classKey))return false;items=[...items,{id:crypto.randomUUID(),classKey,parentId,author:'You (prototype)',text}];try{globalThis.localStorage?.setItem(key,JSON.stringify(items));}catch{}return true;}
