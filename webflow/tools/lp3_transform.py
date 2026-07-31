import json, re, sys, html as htmlmod
from html.parser import HTMLParser

SOURCE = r"C:\Users\gombc\Downloads\preview (3).html"

class Rewriter(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.out=[]
    def handle_starttag(self, tag, attrs):
        vals=dict(attrs); classes=vals.get('class','').split()
        classes=['lp3x-all',f'lp3x-tag-{tag}']+[f'lp3x-{c}' for c in classes]
        if vals.get('id'): classes.append('lp3x-id-'+vals['id'])
        attrs2=[]
        for k,v in attrs:
            if k=='class': continue
            if k=='id': v='lp3x-'+v
            elif k in ('href','aria-controls') and v and v.startswith('#'): v='#lp3x-'+v[1:]
            elif k=='aria-controls' and v: v='lp3x-'+v
            attrs2.append((k,v))
        attrs2.insert(0,('class',' '.join(dict.fromkeys(classes))))
        text='<'+tag+''.join(' '+k+(('="'+v.replace('&','&amp;').replace('"','&quot;')+'"') if v is not None else '') for k,v in attrs2)+'>'
        if tag=='input' and 'lp3x-roster-search' in classes: text='<form class="lp3x-all lp3x-tag-form lp3x-roster-search-form">'+text+'</form>'
        self.out.append(text)
    def handle_startendtag(self, tag, attrs): self.handle_starttag(tag,attrs)
    def handle_endtag(self, tag):
        if tag!='input': self.out.append(f'</{tag}>')
    def handle_data(self,d): self.out.append(d)
    def handle_entityref(self,n): self.out.append('&'+n+';')
    def handle_charref(self,n): self.out.append('&#'+n+';')
    def handle_comment(self,d): self.out.append('<!--'+d+'-->')

def transform_selector(sel):
    sel=sel.strip()
    sel=sel.replace(':root','.lp3x-root')
    sel=re.sub(r'#([A-Za-z_][\w-]*)',r'.lp3x-id-\1',sel)
    sel=re.sub(r'\.([A-Za-z_][\w-]*)',lambda m: '.lp3x-'+m.group(1) if not m.group(1).startswith('lp3x-') else m.group(0),sel)
    sel=re.sub(r'(?<![-\w.#:])\*(?![\w-])','.lp3x-all',sel)
    tags='html body header main footer section article aside nav div span p a button h1 h2 h3 h4 strong small form input label details summary img br svg path line circle'
    for tag in tags.split():
        sel=re.sub(rf'(?<![-\w.#:]){tag}(?![-\w])',f'.lp3x-tag-{tag}',sel)
    if '[' in sel: return None
    # Webflow's native WHTML style importer accepts one class selector per rule.
    # Compound, descendant, and state selectors are handled later by dedicated
    # native classes or the approved interaction runtime.
    if not re.fullmatch(r'\.[A-Za-z_][\w-]*', sel): return None
    return sel

def transform_css(css):
    css=css.replace('!important','')
    literals={
      '--paper':'#f4f0e8','--paper-2':'#e9e2d6','--cream':'#fffaf0','--ink':'#17231e',
      '--forest':'#0e3028','--forest-2':'#17483c','--moss':'#697a62','--sage':'#a9b39e',
      '--rust':'#a65337','--gold':'#c49a55','--line':'rgba(23,35,30,.18)',
      '--line-dark':'rgba(255,250,240,.22)','--shadow':'0 22px 60px rgba(14,48,40,.12)',
      '--max':'1280px','--serif':'Georgia, "Times New Roman", serif','--sans':'Arial, Helvetica, sans-serif'
    }
    for name,value in sorted(literals.items(),key=lambda x:-len(x[0])):
        css=css.replace(f'var({name})',value)
    css=re.sub(r'@keyframes\s+ticker\s*\{\s*to\s*\{[^}]*\}\s*\}','',css,flags=re.S)
    css=css.replace('@media (max-width: 1040px)','@media screen and (max-width: 991px)')
    css=css.replace('@media (max-width: 760px)','@media screen and (max-width: 767px)')
    css=css.replace('@media (max-width: 480px)','@media screen and (max-width: 479px)')
    css=re.sub(r'@media\s*\(prefers-reduced-motion:\s*reduce\)\s*\{.*?\n\s*\}','',css,flags=re.S)
    def walk(text):
        out=[]; i=0
        while i<len(text):
            while i<len(text) and text[i].isspace(): i+=1
            if i>=len(text): break
            b=text.find('{',i)
            if b<0: break
            head=text[i:b].strip(); depth=1; j=b+1
            while j<len(text) and depth:
                depth += (text[j]=='{')-(text[j]=='}'); j+=1
            body=text[b+1:j-1]
            if head.startswith('@media'):
                inner=walk(body)
                if inner: out.append(head+'{'+inner+'}')
            elif not head.startswith('@'):
                sels=[transform_selector(x) for x in head.split(',')]
                sels=[x for x in sels if x]
                if sels: out.append(','.join(sels)+'{'+body+'}')
            i=j
        return '\n'.join(out)
    return '.lp3x-root{background-color:#f4f0e8;color:#17231e;font-family:Arial,Helvetica,sans-serif;line-height:1.55;}\n'+walk(css)

src=open(SOURCE,encoding='utf-8').read()
css=re.search(r'<style>(.*?)</style>',src,re.S).group(1)
body=re.search(r'<body>(.*?)<script>',src,re.S).group(1)

def fragment_payload():
    data=json.loads(re.search(r'const siteData\s*=\s*(\{.*?\n\s*\});',src,re.S).group(1))
    e=lambda v: htmlmod.escape(str(v),quote=True)
    stats=data['stats']
    hero=[(stats['horses'],'horses in full record'),(stats['medianClassesPerHorse'],'median classes per horse'),(f"{stats['shortWindowTopThree']}/{stats['shortWindowHorses']}",'short-window horses with top-three'),(f"{stats['shortWindowWins']}/{stats['shortWindowHorses']}",'short-window horses with a win')]
    record=[(stats['horses'],'Horses in the complete competition record'),(stats['competitions'],'Competitions represented in horse_stories'),(stats['horsesWithWin'],'Horses with at least one recorded win'),(stats['cleanClasses'],'Non-ignored classes in coach-facing rates')]
    audit=[(stats['horses'],'CMS horse records'),(stats['publishedStories'],'Published stories'),(stats['featuredStories'],'Expanded stories'),(stats['anchorStories'],'Required anchors')]
    f={}
    f['hero-stats']=''.join(f'<div class="hero-stat"><strong>{e(v)}</strong><span>{e(l)}</span></div>' for v,l in hero)
    f['record-stats']=''.join(f'<article class="number-card"><strong>{e(v)}</strong><span>{e(l)}</span></article>' for v,l in record)
    f['distribution-bars']=''.join(f'<div class="bar-row"><div class="bar-label">{e(x["label"])}</div><div class="bar-track"><div class="bar-fill" data-width="{x["value"]/stats["horses"]*100}"></div></div><div class="bar-value">{x["value"]}</div></div>' for x in data['partnershipBands'])
    f['cms-audit']=''.join(f'<div class="cms-audit-item"><strong>{e(v)}</strong><span>{e(l)}</span></div>' for v,l in audit)
    f['featured-grid']=''.join(f'<article class="featured-card{(" is-anchor" if x["type"]=="Anchor partnership" else "")}"><div class="featured-kicker"><span>{e(x["type"])}</span><span>{e(x["lane"])}</span></div><h3>{e(x["registered"])}</h3><div class="featured-barn">Barn name: {e(x["barn"])}</div><div class="featured-headline">{e(x["headline"])}</div><p class="featured-story">{e(x["story"])}</p><div class="featured-evidence">{e(x["evidence"])}</div></article>' for x in data['featured'])
    roster=[x for x in data['roster'] if x['status']=='Publish'][:12]
    f['roster-grid']=''.join(f'<details class="roster-card"><summary><span class="roster-lane">{e(x["lane"])}</span><h3>{e(x["registered"])}</h3><div class="roster-barn">{("Barn name: "+e(x["barn"]) if x["barn"] else "Registered name only")}</div><div class="roster-numbers"><div><strong>{x["classes"]}</strong><span>Classes</span></div><div><strong>{x["competitions"]}</strong><span>Competitions</span></div></div></summary><div class="roster-detail"><p>{e(x["story"])}</p><div class="micro">{e(x["disciplines"])}</div></div></details>' for x in roster)
    f['class-list']=''.join(f'<article class="class-row"><div class="placing">{e(x["placing"])}</div><div class="class-horse"><strong>{e(x["horse"])}</strong><span>{e(x["barn"])} · {x["entries"]} entries</span></div><div class="class-title">{e(x["title"])}</div><div class="class-date">{e(x["date"])}</div><div class="percentile-pill">{e(x["context"])}</div></article>' for x in data['classes'])
    metrics=[('Wins',stats['wins'],round(stats['wins']/stats['cleanClasses']*100,1)),('Top three',stats['topThree'],round(stats['topThree']/stats['cleanClasses']*100,1)),('Top eight',stats['topEight'],round(stats['topEight']/stats['cleanClasses']*100,1))]
    f['result-bars']=''.join(f'<div class="result-item"><div class="result-head"><span>{l} · {c} classes</span><strong>{p}%</strong></div><div class="result-track"><div class="result-fill" data-width="{p}"></div></div></div>' for l,c,p in metrics)
    out={}
    for k,v in f.items():
        q=Rewriter(); q.feed(v); out[k]=''.join(q.out)
    return json.dumps(out,separators=(',',':'))

def runtime_payload():
    js=re.findall(r'<script>(.*?)</script>',src,re.S)[-1]
    ids=['hero-stats','record-stats','distribution-bars','cms-audit','featured-grid','roster-grid','roster-search','roster-more','class-list','result-bars','site-nav']
    for ident in ids: js=js.replace('#'+ident,'#lp3x-'+ident)
    def classes(m):
        names=[]
        for token in m.group(1).split():
            if '${' in token: names.append(token)
            else: names.append('lp3x-'+token)
        return 'class="'+' '.join(names)+'"'
    js=re.sub(r'class="([^"]*)"',classes,js)
    for name in ['menu-button','nav-links']:
        js=js.replace('.'+name,'.lp3x-'+name)
    js=js.replace('"is-active"','"lp3x-is-active"').replace('"is-open"','"lp3x-is-open"')
    css='''<style>
@keyframes lp3xTicker{to{transform:translateX(-50%)}}
.lp3x-coach-strip-track{animation:lp3xTicker 28s linear infinite}
.lp3x-filter-button.lp3x-is-active{background:#17231e;color:#fffaf0;border-color:#17231e}
.lp3x-nav-links.lp3x-is-open{transform:translateY(0);opacity:1;pointer-events:auto}
.lp3x-bar-fill,.lp3x-result-fill{transition:width .9s ease}
@media(prefers-reduced-motion:reduce){.lp3x-coach-strip-track{animation:none}.lp3x-bar-fill,.lp3x-result-fill{transition:none}}
</style>'''
    return css+'<script>'+js+'</script>'
p=Rewriter(); p.feed(body)
html='<div class="lp3x-all lp3x-tag-div lp3x-root" id="lp3x-root">'+''.join(p.out)+'</div>'
payload=runtime_payload() if '--runtime' in sys.argv else (fragment_payload() if '--fragments' in sys.argv else json.dumps({'html':html,'css':transform_css(css)},separators=(',',':')))
sys.argv=[x for x in sys.argv if x not in ('--fragments','--runtime')]
if len(sys.argv)==3:
    start=int(sys.argv[1]); size=int(sys.argv[2]); print(payload[start:start+size],end='')
elif len(sys.argv)==2 and sys.argv[1]=='--length': print(len(payload))
else: print(payload)
