export const config = {
  runtime: "edge"
};

const html = String.raw`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WEC Comments</title>
<style>
html,body{margin:0;padding:0;background:transparent;color:#111;font-family:Arial,sans-serif}
*{box-sizing:border-box}
.wc{max-width:760px;margin:0 auto;padding:8px}
.top{border:1px solid #d7dadd;border-radius:10px;background:#fff;margin-bottom:10px;overflow:hidden}
.head{background:#dcb6d1;padding:10px 12px;font-weight:800}
.body{padding:10px 12px}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
button{font:inherit;font-weight:800;border:1px solid #111;border-radius:999px;background:#fff;padding:8px 12px;cursor:pointer}
button.primary,.pill.active{background:#815374;border-color:#815374;color:#fff}
button.ghost{border-color:#d0d0d0}
input,textarea{font:inherit;border:1px solid #bfc3c7;border-radius:8px;padding:9px 10px}
input{min-width:170px}
textarea{width:100%;min-height:76px;margin-top:8px}
.muted{font-size:12px;color:#666;word-break:break-word}
.crumbs{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin:8px 0}
.crumb{border:1px solid #d0d0d0;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:800;background:#fff}
.grid{display:grid;gap:8px}
.card{width:100%;text-align:left;border-radius:10px;border:1px solid #d7dadd;background:#fff;padding:10px 12px}
.card .title{font-weight:800}
.card .sub{font-size:12px;color:#555;margin-top:3px}
.card.entry{display:grid;grid-template-columns:44px 1fr;gap:8px;align-items:start}
.card.entry .sub{grid-column:2}
.entryOrder{font-weight:800;color:#555}
.cwf-entry{border-color:#815374;background:#fbf7fa}
.cwfBadge{display:inline-block;margin-right:6px;border-radius:999px;background:#815374;color:#fff;font-size:11px;font-weight:800;line-height:1;padding:4px 7px;vertical-align:middle}
.commentBox{border:1px solid #d7dadd;border-radius:10px;background:#fff;padding:10px 12px;margin-top:10px}
.status{padding:8px 10px;border-radius:8px;background:#f5f5f5;font-weight:800;margin-top:8px}
.ok{background:#edf7ef;color:#17612f}
.bad{background:#fff0f0;color:#9b1c1c}
.comments{margin-top:8px;display:grid;gap:6px}
.comment{border-top:1px solid #eee;padding-top:6px;font-size:13px}
.sessions{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.sessionPill{border:1px solid #d0d0d0;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:800;background:#fff}
.hidden{display:none}
</style>
</head>
<body>
<div class="wc">
  <div class="top">
    <div class="head" id="title">WEC Comments</div>
    <div class="body">
      <div class="row">
        <button class="primary" id="startBtn">Start session</button>
        <input id="nameInput" placeholder="Name">
        <button id="saveNameBtn">Save name</button>
      </div>
      <div class="muted" id="sessionLine">No session.</div>
      <div class="muted" id="focusLine">Loading focus day.</div>
      <div class="sessions" id="sessionsList"></div>
    </div>
  </div>
  <div class="crumbs" id="crumbs"></div>
  <div class="grid" id="list"></div>
  <div class="commentBox hidden" id="commentBox">
    <div class="muted" id="scopeLine"></div>
    <textarea id="commentText" placeholder="Comment..."></textarea>
    <div class="row"><button class="primary" id="saveCommentBtn">Save comment</button></div>
    <div class="status" id="status">Ready.</div>
    <div class="comments" id="comments"></div>
  </div>
</div>
<script>
(function(){
  var editUrl="/test/wec-schedule/edit";
  var stateUrl="/test/wec-schedule/comment-state";
  var dayMs=86400000, ttl=180*dayMs;
  var state=[], rings=[], selectedRing=null, selectedClass=null, selectedEntry=null, sessions=[];
  var device=getDevice(), session=getSession(), comments=[];
  var el=function(id){return document.getElementById(id)};
  el("startBtn").onclick=startSession;
  el("saveNameBtn").onclick=saveName;
  el("saveCommentBtn").onclick=saveComment;
  el("nameInput").value=device.user_name||"";
  load();

  function load(){
    fetch(stateUrl).then(function(r){return r.json()}).then(function(payload){
      state=payload&&payload.ok?payload:{rings:[]};
      rings=Array.isArray(state.rings)?state.rings:[];
      el("title").textContent=state.show_name||"WEC Comments";
      el("focusLine").textContent="Focus day: "+(state.focus_day||"")+" / "+(state.class_count||0)+" classes / "+(state.entry_count||0)+" entries";
      renderSession(); render();
      listSessions();
      if(session.session_id) listComments();
    }).catch(function(e){setStatus("Schedule failed to load","bad",e)});
  }

  function startSession(){
    if(!device.user_name){device.user_name=randomName(); saveDevice(device); el("nameInput").value=device.user_name}
    session={session_id:"session_"+Date.now()+"_"+Math.random().toString(16).slice(2),started_at:Date.now()};
    saveSession(session);
    post({action:"start-session",session_id:session.session_id,device_id:device.device_id,user_name:device.user_name,show_no:state.show_no||14906,focus_day:state.focus_day,page:"wec-comments",source:"wec-comments-widget"})
      .then(function(r){setStatus("Session started","ok",r);renderSession();listSessions();listComments()}).catch(function(e){setStatus("Session failed","bad",e)});
  }

  function saveName(){
    var name=el("nameInput").value.trim();
    if(!name)return setStatus("Name is empty","bad",{});
    device.user_name=name; saveDevice(device);
    if(session.session_id){
      post({action:"start-session",session_id:session.session_id,device_id:device.device_id,user_name:device.user_name,show_no:state.show_no||14906,focus_day:state.focus_day,page:"wec-comments",source:"wec-comments-widget"})
        .then(function(r){setStatus("Name saved","ok",r);renderSession();listSessions()}).catch(function(e){setStatus("Name save failed","bad",e)});
    } else {
      setStatus("Name saved","ok",{user_name:name}); renderSession();
    }
  }

  function saveComment(){
    if(!session.session_id)return setStatus("Start session first","bad",{});
    var scope=currentScope();
    if(!scope)return setStatus("Select ring, class, or entry first","bad",{});
    var text=el("commentText").value.trim();
    if(!text)return setStatus("Comment is empty","bad",{});
    post({action:"add-comment",session_id:session.session_id,device_id:device.device_id,user_name:device.user_name,show_no:state.show_no||14906,focus_day:state.focus_day,comment_scope:scope.comment_scope,ring_no:scope.ring_no||"",class_no:scope.class_no||"",entry_no:scope.entry_no||"",comment_text:text,source:"wec-comments-widget"})
      .then(function(r){el("commentText").value="";setStatus("Comment saved","ok",r);listComments()}).catch(function(e){setStatus("Comment failed","bad",e)});
  }

  function listComments(){
    post({action:"list-comments",show_no:state.show_no||14906}).then(function(r){comments=r.records||[];renderComments()}).catch(function(){});
  }

  function listSessions(){
    post({action:"list-sessions",show_no:state.show_no||14906,focus_day:state.focus_day,active_window_minutes:180})
      .then(function(r){sessions=r.records||[];renderSessions()}).catch(function(){});
  }

  function render(){
    renderCrumbs();
    var list=el("list"); list.innerHTML="";
    if(!selectedRing){
      rings.forEach(function(r){list.appendChild(card(r.ring_name||r.name,(r.class_count+" classes / "+r.entry_count+" entries"),function(){selectedRing=r;selectedClass=null;selectedEntry=null;render()}))});
    } else if(!selectedClass){
      selectedRing.classes.forEach(function(c){list.appendChild(card((c.start_display||"check time")+"  "+c.class_number+" - "+c.class_name,(c.entries.length+" entries"),function(){selectedClass=c;selectedEntry=null;render()}))});
    } else {
      selectedClass.entries.forEach(function(e){list.appendChild(entryCard(e,function(){selectedEntry=e;render()}))});
      if(!selectedClass.entries.length)list.appendChild(card("No entries listed","Comment on the class instead",function(){}));
    }
    renderCommentBox();
  }

  function renderCrumbs(){
    var c=el("crumbs"); c.innerHTML="";
    c.appendChild(crumb("Rings",function(){selectedRing=null;selectedClass=null;selectedEntry=null;render()}));
    if(selectedRing)c.appendChild(crumb(selectedRing.ring_name||selectedRing.name,function(){selectedClass=null;selectedEntry=null;render()}));
    if(selectedClass)c.appendChild(crumb(selectedClass.class_number,function(){selectedEntry=null;render()}));
    if(selectedEntry)c.appendChild(crumb(selectedEntry.horse_display||selectedEntry.horse,function(){}));
  }

  function renderCommentBox(){
    var box=el("commentBox"), scope=currentScope();
    box.classList.toggle("hidden",!scope);
    el("scopeLine").textContent=scope?("Commenting on "+scope.comment_scope+": "+scope.label):"";
    renderComments();
  }

  function renderComments(){
    var scope=currentScope(), out=el("comments"); out.innerHTML="";
    if(!scope)return;
    comments.filter(function(c){
      return String(c.comment_scope)===scope.comment_scope
        && (!scope.ring_no||String(c.ring_no)===String(scope.ring_no))
        && (!scope.class_no||String(c.class_no)===String(scope.class_no))
        && (!scope.entry_no||String(c.entry_no)===String(scope.entry_no));
    }).slice(0,8).forEach(function(c){
      var div=document.createElement("div"); div.className="comment";
      div.textContent=(c.user_name||"User")+": "+(c.comment_text||"");
      out.appendChild(div);
    });
  }

  function renderSessions(){
    var out=el("sessionsList"); out.innerHTML="";
    if(!sessions.length){var empty=document.createElement("span");empty.className="muted";empty.textContent="No active sessions shown.";out.appendChild(empty);return}
    sessions.forEach(function(s){
      var span=document.createElement("span"); span.className="sessionPill";
      span.textContent=s.user_name||"Guest";
      out.appendChild(span);
    });
  }

  function currentScope(){
    if(selectedEntry)return {comment_scope:"entry",label:selectedEntry.horse_display||selectedEntry.horse,ring_no:selectedRing.ring_no,class_no:selectedClass.class_no,entry_no:selectedEntry.entry_no};
    if(selectedClass)return {comment_scope:"class",label:selectedClass.class_number+" - "+selectedClass.class_name,ring_no:selectedRing.ring_no,class_no:selectedClass.class_no};
    if(selectedRing)return {comment_scope:"ring",label:selectedRing.name,ring_no:selectedRing.ring_no};
    return null;
  }

  function card(title,sub,fn){var b=document.createElement("button");b.className="card";b.type="button";b.onclick=fn;b.innerHTML='<div class="title"></div><div class="sub"></div>';b.children[0].textContent=title;b.children[1].textContent=sub||"";return b}
  function entryCard(entry,fn){var b=document.createElement("button");b.className="card entry "+(entry.entry_class||"");b.type="button";b.onclick=fn;b.innerHTML='<div class="entryOrder"></div><div class="title"></div><div class="sub"></div>';b.children[0].textContent=entry.entry_order||"";b.children[1].innerHTML=(entry.is_cwf?'<span class="cwfBadge">'+escapeHtml(entry.trainer_display||"CWF")+'</span>':"")+escapeHtml(entry.horse_display||entry.horse||"Entry");b.children[2].textContent="Entry "+(entry.entry_no||"")+" / "+(entry.rider_display||entry.rider||"");return b}
  function escapeHtml(value){return String(value||"").replace(/[&<>"']/g,function(ch){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]})}
  function crumb(text,fn){var b=document.createElement("button");b.className="crumb";b.type="button";b.onclick=fn;b.textContent=text;return b}
  function post(payload){return fetch(editUrl,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)}).then(function(r){return r.text().then(function(t){var d;try{d=JSON.parse(t)}catch(e){d={raw:t}};if(!r.ok||d.ok===false)throw d;return d})})}
  function renderSession(){el("sessionLine").textContent=(session.session_id?"Session active":"No session")+" / "+(device.user_name||"No name")+" / "+device.device_id}
  function setStatus(text,kind,obj){var s=el("status"); if(!s)return; s.className="status "+(kind||""); s.textContent=text}
  function randomName(){return "Guest "+Math.floor(1000+Math.random()*9000)}
  function getDevice(){var d=readCookie("comments-session")||localStorage.getItem("comments-device");try{d=d?JSON.parse(d):null}catch(e){d=null}return d&&d.device_id?d:{device_id:"device_"+Date.now()+"_"+Math.random().toString(16).slice(2),user_name:""}}
  function saveDevice(d){var v=JSON.stringify(d);localStorage.setItem("comments-device",v);writeCookie("comments-session",v)}
  function getSession(){try{return JSON.parse(localStorage.getItem("comments-current-session")||"{}")}catch(e){return {}}}
  function saveSession(s){localStorage.setItem("comments-current-session",JSON.stringify(s))}
  function readCookie(n){var m=document.cookie.match(new RegExp("(?:^|; )"+n.replace(/[-[\]{}()*+?.,\\^$|#\s]/g,"\\$&")+"=([^;]*)"));return m?decodeURIComponent(m[1]):""}
  function writeCookie(n,v){document.cookie=n+"="+encodeURIComponent(v)+"; Max-Age="+Math.floor(ttl/1000)+"; Path=/; SameSite=Lax"}
})();
</script>
</body>
</html>`;

export const GET = async () => new Response(html, {
  headers: {
    "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
    "Content-Type": "text/html; charset=utf-8"
  }
});
