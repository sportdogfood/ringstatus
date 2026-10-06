export type MessageRow = {name:string;classIds:string[];status:string;start?:string;tokens:{label:string;value:string}[];entries:string[]};
export function normalizeMessage(body:string, alert?:string):MessageRow[] {
 const lines=body.split('\n').map(v=>v.trim()).filter(Boolean);
 const rows:MessageRow[]=[];
 const name=(value:string)=>{const ids=value.match(/\((\d+(?:,\s*\d+)*)\)/);return {name:value.replace(/\s*\((\d+(?:,\s*\d+)*)\)/,'').replace(/\.$/,'').trim(),classIds:ids?ids[1].split(/,\s*/):[]};};
 const make=(value:string,status:string):MessageRow=>({...name(value),status,tokens:[],entries:[]});
 const fields=(line:string,row:MessageRow)=>{for(const part of line.split('|')){const m=part.trim().match(/^([^:]+):\s*(.+)$/);if(!m)continue;const label=m[1].trim(),value=m[2].trim();if(/^(Start|Starts|Go)$/.test(label))row.start=value;else row.tokens.push({label:/^(Ends|Projected end)$/.test(label)?'Ends in':/^(In|Till|Mins till go)$/.test(label)?'Starts in':label,value});}};
 for(const line of lines){
  const record=line.match(/^(Now|Next|First|Current group):\s*(.+)$/i);
  if(record){rows.push(make(record[2],record[1]==='Current group'?'NOW':record[1].toUpperCase()));continue;}
  if(/^(Start|Starts|Go|\|?Gone):/.test(line)&&rows.length){fields(line,rows.at(-1)!);continue;}
 }
 if(rows.length){const late=lines.find(v=>/^Running Late/i.test(v));if(late)rows[0].tokens.unshift({label:'Late',value:late.replace(/^Running Late about /i,'')});return rows;}
 const compact=lines.find(v=>/^\d{1,2}:\d{2}\s*[AP]M?\s*\|/i.test(v));
 if(compact){const parts=compact.split('|').map(v=>v.trim());const row=make(parts[2],alert==='2nd reminder'||lines.some(v=>/2nd Reminder/i.test(v))?'2ND':'NEXT');row.start=parts[0];row.entries=parts.slice(3).flatMap(v=>v.split(/,\s*/));return [row];}
 const trip=body.match(/^.+? — (Ring \d+)\. ([^.]+)\. (.+?)\. Scheduled ([^.]+)\. Latest go ([^.]+)\.(?: Ring walk ([^.]+)\.)?$/);
 if(trip){const row=make(trip[3].replace(/\. Back #\d+$/,''),'GO');const back=trip[3].match(/Back #(\d+)/);row.start=trip[4];row.entries=[trip[2]];row.tokens=[{label:'Ring',value:trip[1].replace('Ring ','')},{label:'Latest go',value:trip[5]}];if(back)row.tokens.push({label:'Back #',value:back[1]});if(trip[6])row.tokens.push({label:'Ring walk',value:trip[6]});return [row];}
 if(/^Go:/.test(body)){const row=make('Go time','GO');fields(body,row);return [row];}
 return [];
}
