import {connect} from 'cloudflare:sockets';
const encoder=new TextEncoder();
function unchunk(bytes){
 const chunks=[];let offset=0,total=0;
 while(offset<bytes.length){
  let end=offset;while(end+1<bytes.length&&!(bytes[end]===13&&bytes[end+1]===10))end++;
  if(end+1>=bytes.length)throw Error('Incomplete chunk size');
  const sizeText=new TextDecoder().decode(bytes.slice(offset,end)).split(';')[0];
  if(!/^[0-9a-f]+$/i.test(sizeText))throw Error('Invalid chunk size');
  const size=parseInt(sizeText,16);offset=end+2;if(size===0){const out=new Uint8Array(total);let i=0;for(const c of chunks){out.set(c,i);i+=c.length;}return out;}
  if(offset+size+2>bytes.length||bytes[offset+size]!==13||bytes[offset+size+1]!==10)throw Error('Incomplete chunk body');
  chunks.push(bytes.slice(offset,offset+size));total+=size;offset+=size+2;
 }
 throw Error('Missing terminal chunk');
}
export async function tcpFetch(input,options={}){
 const url=new URL(input);if(url.protocol!=='https:'||!['sglapi.wellingtoninternational.com','tls.peet.ws'].includes(url.hostname))throw Error('Unapproved diagnostic destination');
 const socket=connect({hostname:url.hostname,port:443},{secureTransport:'on'});socket.closed.catch(()=>{});
 let timedOut=false;const timer=setTimeout(()=>{timedOut=true;socket.close().catch(()=>{});},20000);
 try{
  await socket.opened;
  const headers=new Headers(options.headers);headers.set('Host',url.hostname);headers.set('Connection','close');headers.set('Accept-Encoding','identity');headers.delete('cf-worker');
  let wire='GET '+url.pathname+url.search+' HTTP/1.1\r\n';for(const [k,v]of headers){if(/[\r\n]/.test(k+v))throw Error('Invalid header');wire+=k+': '+v+'\r\n';}wire+='\r\n';
  const writer=socket.writable.getWriter();await writer.write(encoder.encode(wire));writer.releaseLock();
  const reader=socket.readable.getReader();const chunks=[];let size=0;
  while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>5*1024*1024)throw Error('Diagnostic response too large');chunks.push(value);}
  if(timedOut)throw Error('Diagnostic socket timed out');
  const raw=new Uint8Array(size);let cursor=0;for(const c of chunks){raw.set(c,cursor);cursor+=c.length;}
  let boundary=-1;for(let i=0;i+3<raw.length;i++)if(raw[i]===13&&raw[i+1]===10&&raw[i+2]===13&&raw[i+3]===10){boundary=i;break;}
  if(boundary<0)throw Error('Missing HTTP headers');
  const lines=new TextDecoder().decode(raw.slice(0,boundary)).split('\r\n');const status=Number(lines.shift().match(/^HTTP\/1\.[01] (\d{3}) /)?.[1]);if(!status)throw Error('Invalid HTTP status');
  const responseHeaders=new Headers();for(const line of lines){const i=line.indexOf(':');if(i<=0)throw Error('Invalid HTTP header');responseHeaders.append(line.slice(0,i),line.slice(i+1).trim());}
  let body=raw.slice(boundary+4);if(responseHeaders.get('transfer-encoding')?.toLowerCase()==='chunked'){body=unchunk(body);responseHeaders.delete('transfer-encoding');}
  else if(responseHeaders.has('content-length')&&Number(responseHeaders.get('content-length'))!==body.length)throw Error('Incomplete HTTP body');
  const compression=responseHeaders.get('content-encoding');if(compression&&compression!=='identity')throw Error('Unexpected response compression');
  responseHeaders.delete('content-length');return new Response(body,{status,headers:responseHeaders});
 }finally{clearTimeout(timer);await socket.close().catch(()=>{});}
}
