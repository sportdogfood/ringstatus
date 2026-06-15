(function(){
  function install(){
    if(location.pathname.replace(/\/$/,"")!=="/wec-sessions")return;
    if(document.querySelector('iframe[src="/test/wec-schedule/session-widget"]'))return;
    var start=document.getElementById("wc-start");
    var out=document.getElementById("wc-out");
    var target=start&&start.closest("div");
    if(!target&&out)target=out.closest("div");
    if(!target)target=document.body;
    var iframe=document.createElement("iframe");
    iframe.src="/test/wec-schedule/session-widget";
    iframe.title="WEC Session Comments";
    iframe.loading="eager";
    iframe.style.cssText="width:100%;height:620px;border:0;display:block;overflow:hidden;";
    if(target===document.body){
      document.body.innerHTML="";
      document.body.appendChild(iframe);
    }else{
      target.replaceWith(iframe);
    }
  }
  if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",install);
  else install();
})();
