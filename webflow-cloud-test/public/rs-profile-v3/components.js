// V3 presentation components. Profile and Schedule consume this module and the same styles.css/primitives.css.
// Content, identity, data loading and navigation remain the responsibility of each screen.
const escapeAttribute=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function v3Button(action,text,options={},icon=()=> ''){return `<button type="button" class="ui-button" data-action="${action}" ${options.variant?`data-variant="${options.variant}"`:''} ${options.label?`aria-label="${escapeAttribute(options.label)}"`:''} ${options.extra||''}>${options.icon?icon(options.icon):''}${text}</button>`;}
export function v3Brand(controls=''){return `<div class="brand-row" data-stack="brand"><span class="profile-brand" aria-label="RingStatus Pro"><span class="rs-wordmark" aria-hidden="true"><span class="rs-wordmark-ring">RING</span><span class="rs-wordmark-status">STATUS</span></span><span class="profile-pro">PRO</span></span>${controls}</div>`;}
export function v3Back(content){return `<div class="back-row" data-stack="back">${content}</div>`;}
export function v3Footer(content){return `<footer class="footer-row" data-stack="footer">${content}</footer>`;}
export function v3Shell(top,body,footer,bottom=''){return `<header class="stack stack-top">${top}</header><div class="stack-scroll"><div class="stack stack-body">${body}</div></div>${bottom}${footer}`;}
export function v3Control(element,{icon=false}={}){element.classList.add(icon?'icon-button':'ui-button');if(!icon)element.dataset.v3Control='action';element.querySelectorAll('svg').forEach(svg=>svg.classList.add('icon'));return element;}

export function v3Search(label,id,icon,placeholder='Search…'){return `<label class="search-control">${icon}<input type="search" id="${id}" aria-label="${escapeAttribute(label)}" placeholder="${escapeAttribute(placeholder)}" autocomplete="off"></label>`;}
