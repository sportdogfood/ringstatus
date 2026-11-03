import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {Workbook,SpreadsheetFile} from '@oai/artifact-tool';
const out=path.dirname(fileURLToPath(import.meta.url));
const data=JSON.parse(await fs.readFile(path.join(out,'attachment-data.json'),'utf8'));
const wb=Workbook.create();
const summary=wb.worksheets.add('Summary');
const fresh=wb.worksheets.add('25 new complaints');
const all=wb.worksheets.add('All 209 complaints');
const navy='#24364B', ink='#172B40', pale='#FFF2CE';
function base(sheet,end,cols){sheet.showGridLines=false;sheet.getRange(`A1:${cols}${end}`).format.font={name:'Arial',size:10,color:ink};sheet.getRange(`A1:${cols}${end}`).format.verticalAlignment='top';}
function head(sheet,range){sheet.getRange(range).format={fill:navy,font:{name:'Arial',size:10,bold:true,color:'#FFFFFF'},wrapText:true,horizontalAlignment:'center',verticalAlignment:'center'};}
function title(sheet,value){sheet.getRange('A2').values=[[value]];sheet.getRange('A2').format.font={name:'Arial',size:14,bold:true,color:ink};sheet.getRange('A2:H2').format.rowHeight=24;}
const headers=['Reference','Logged UTC date','Added since 2 Nov','Common pattern','Complaint excerpt','Recorded correction state','Proposed method / acceptance check','Source record'];
const widths=[20,14,16,45,76,27,70,43];
function detail(sheet,records,name,titleText){
 const end=records.length+5;base(sheet,end,'H');title(sheet,titleText);
 sheet.getRange('A3').values=[['Review snapshot: 3 November 2026. Source: owner complaint register. New records are not necessarily new incident dates.']];
 sheet.getRange('A3').format.font={name:'Arial',size:10,italic:true,color:'#526174'};
 sheet.getRange('A5:H5').values=[headers];
 const matrix=records.map(r=>[r.ref,new Date(r.created),r.new?'Yes':'No',r.group,r.excerpt,r.remediation,r.method,r.url]);
 sheet.getRange(`A6:H${end}`).values=matrix;
 const table=sheet.tables.add(`A5:H${end}`,true,name);table.showFilterButton=true;
 head(sheet,'A5:H5');sheet.getRange('A5:H5').format.rowHeight=34;
 for(let i=0;i<8;i++)sheet.getRangeByIndexes(0,i,end,1).format.columnWidth=widths[i];
 sheet.getRange(`A6:H${end}`).format.wrapText=true;
 sheet.getRange(`B6:B${end}`).setNumberFormat('yyyy-mm-dd');
 sheet.getRange(`C6:C${end}`).format.horizontalAlignment='center';
 sheet.getRange(`H6:H${end}`).format.font={name:'Arial',size:10,color:'#245A83'};
 sheet.getRange(`C6:C${end}`).conditionalFormats.add('containsText',{text:'Yes',format:{fill:pale}});
 for(let i=0;i<matrix.length;i++){
  const lines=Math.max(...[0,3,4,5,6,7].map(j=>Math.ceil(String(matrix[i][j]??'').length/Math.max(10,widths[j]-5))));
  sheet.getRange(`A${i+6}:H${i+6}`).format.rowHeight=Math.max(52,lines*15+10);
 }
 sheet.freezePanes.freezeRows(5);sheet.freezePanes.freezeColumns(1);
 return {rows:records.length,table:name};
}
const freshRecords=data.records.filter(r=>r.new);
detail(fresh,freshRecords,'NewComplaints','25 complaints added since the 2 November review');
detail(all,data.records,'AllComplaints','Complaint register reviewed on 3 November 2026');
base(summary,34,'E');summary.tabColor=navy;
summary.getRange('A1:A34').format.columnWidth=52;
summary.getRange('B1:D34').format.columnWidth=16;
summary.getRange('E1:E34').format.columnWidth=90;
summary.getRange('A2').values=[['Codex complaints: recurring issues and new records']];
summary.getRange('A2').format.font={name:'Arial',size:14,bold:true,color:ink};
summary.getRange('A4').values=[['Reviewed 3 November 2026. Comparison: 2 November 2026 review, which contained 184 records.']];
summary.getRange('A4').format.font={name:'Arial',size:10,italic:true,color:'#526174'};
summary.getRange('A6:D6').values=[['Total complaints',null,'Added since 2 Nov',null]];
summary.getRange('B6').formulas=[["=COUNTA('All 209 complaints'!A6:A214)"]];
summary.getRange('D6').formulas=[["=COUNTIFS('All 209 complaints'!C6:C214,\"Yes\")"]];
summary.getRange('A6:D6').format.font={name:'Arial',size:10,bold:true,color:ink};
summary.getRange('B6').format.horizontalAlignment='center';summary.getRange('D6').format.horizontalAlignment='center';
summary.getRange('A8:E8').values=[['Common pattern','Logged by 2 Nov','Added since 2 Nov','Total','Existing corrective approach']];head(summary,'A8:E8');summary.getRange('A8:E8').format.rowHeight=32;
for(let i=0;i<data.groups.length;i++){
 const r=i+9,g=data.groups[i];summary.getRange(`A${r}`).values=[[g.label]];
 summary.getRange(`B${r}:D${r}`).formulas=[[
  `=COUNTIFS('All 209 complaints'!$D$6:$D$214,A${r},'All 209 complaints'!$C$6:$C$214,"No")`,
  `=COUNTIFS('All 209 complaints'!$D$6:$D$214,A${r},'All 209 complaints'!$C$6:$C$214,"Yes")`,
  `=SUM(B${r}:C${r})`]];
 summary.getRange(`E${r}`).values=[[g.method]];
 summary.getRange(`A${r}:E${r}`).format.wrapText=true;
 summary.getRange(`A${r}:E${r}`).format.rowHeight=Math.max(43,Math.ceil(g.method.length/82)*15+10);
 if(i%2===1)summary.getRange(`A${r}:E${r}`).format.fill='#F2F5F8';
}
summary.getRange('A21').values=[['Total']];summary.getRange('B21:D21').formulas=[['=SUM(B9:B20)','=SUM(C9:C20)','=SUM(D9:D20)']];summary.getRange('A21:E21').format.font={name:'Arial',size:10,bold:true,color:ink};
summary.getRange('B9:D21').setNumberFormat('#,##0');summary.getRange('B9:D21').format.horizontalAlignment='right';
const notes=[
 'New means added since the prior review; the incident itself may have occurred earlier.',
 'All 25 new complaints fall in families already present in the earlier 184 records.',
 'Shared families show recurring patterns, not duplicate incidents or a proven common root cause.',
 'Complaint excerpts are shortened. Contact details and local evidence paths are omitted.',
 'Recorded correction states are retained; this attachment does not recertify product repairs.',
 'Latest example: C030 reports promises replacing checks performed during the actual work.',
 'Required before edits: identify the exact approval, including later corrections.',
 'Required before deployment: compare actual changes and obtain required independent review.',
 'Required before stopping: verify through the existing authorized workflow and continue permitted work.'
];
for(let i=0;i<notes.length;i++){summary.getRange(`A${i+24}`).values=[[notes[i]]];summary.getRange(`A${i+24}:E${i+24}`).format.rowHeight=18;}
wb.recalculate();
const totals=summary.getRange('B21:D21').values[0];
if(JSON.stringify(totals)!==JSON.stringify([184,25,209]))throw Error('Reconciliation failed: '+JSON.stringify(totals));
if(summary.getRange('B6').values[0][0]!==209||summary.getRange('D6').values[0][0]!==25)throw Error('Header counts mismatch');
const errors=await wb.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!',options:{useRegex:true,maxResults:20},maxChars:1800});
await fs.writeFile(path.join(out,'formula-scan.json'),errors.ndjson);
for(const [sheet,range,file]of [['Summary','A1:E21','summary-preview.png'],['Summary','A23:E32','notes-preview.png'],['25 new complaints','A1:H8','new-preview.png'],['All 209 complaints','A1:H8','all-preview.png']]){
 const image=await wb.render({sheetName:sheet,range,scale:1,format:'png'});await fs.writeFile(path.join(out,file),new Uint8Array(await image.arrayBuffer()));
}
const output=path.join(out,'OpenAI-complaints-2026-11-03.xlsx');await(await SpreadsheetFile.exportXlsx(wb)).save(output);
await fs.writeFile(path.join(out,'attachment-verification.json'),JSON.stringify({file:output,recordCount:209,newCount:25,commonPatterns:12,totals,sourceSnapshot:'2026-11-03 with latest C030 owner example',sourceIdsDistinct:new Set(data.records.map(r=>r.id)).size,renderedSheets:3,formulaScan:errors.ndjson},null,2));
console.log(JSON.stringify({output,totals,records:209,newRecords:25,renderedSheets:3}));
