from pathlib import Path
from email.message import EmailMessage
from email import policy
from email.parser import BytesParser
import json,hashlib,zipfile
from xml.etree import ElementTree as ET

folder=Path(__file__).resolve().parent
attachment=folder/'OpenAI-complaints-2026-11-03.xlsx'
body=(folder/'email-body.txt').read_text(encoding='utf-8')
message=EmailMessage(policy=policy.SMTP)
message['To']='support@openai.com'
message['Subject']='Human review requested: recurring Codex failures, 209 complaints and 25 newly recorded'
message['X-Unsent']='1'
message.set_content(body)
message.add_attachment(attachment.read_bytes(),maintype='application',subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',filename=attachment.name)
draft=folder/'OpenAI-support-human-review-draft.eml';draft.write_bytes(message.as_bytes())
saved=BytesParser(policy=policy.default).parsebytes(draft.read_bytes())
parts=list(saved.iter_attachments());assert len(parts)==1
assert parts[0].get_payload(decode=True)==attachment.read_bytes()
assert saved.get_body(preferencelist=('plain',)).get_content().replace('\r\n','\n').strip()==body.strip()
assert saved['To']=='support@openai.com'
ns={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
with zipfile.ZipFile(attachment) as z:
 tables=[ET.fromstring(z.read(n)) for n in z.namelist() if n.startswith('xl/tables/') and n.endswith('.xml')]
 ranges={x.attrib['name']:x.attrib['ref'] for x in tables}
 assert ranges['NewComplaints']=='A5:H30' and ranges['AllComplaints']=='A5:H214',ranges
 sheet=ET.fromstring(z.read('xl/worksheets/sheet1.xml'))
 cells={x.attrib['r']:x.find('s:v',ns).text if x.find('s:v',ns) is not None else None for x in sheet.findall('.//s:c',ns)}
 assert [float(cells[k]) for k in ['B21','C21','D21']]==[184,25,209]
 sheets=ET.fromstring(z.read('xl/workbook.xml')).findall('s:sheets/s:sheet',ns)
 assert [s.attrib['name'] for s in sheets]==['Summary','25 new complaints','All 209 complaints']
 verification={'draft':draft.as_posix(),'recipient':str(saved['To']),'attachment':attachment.as_posix(),'attachmentBytes':attachment.stat().st_size,'attachmentSHA256':hashlib.sha256(attachment.read_bytes()).hexdigest(),'mimeAttachmentMatchesWorkbook':True,'workbookTableRanges':ranges,'summaryCounts':[184,25,209],'emailSent':False,'supportAddressSource':'https://status.openai.com/incidents/01M5NAR5GZC5M7D0S7AQ6CGEEN'}
(folder/'email-verification.json').write_text(json.dumps(verification,indent=2)+'\n',encoding='utf-8')
print(json.dumps(verification))
