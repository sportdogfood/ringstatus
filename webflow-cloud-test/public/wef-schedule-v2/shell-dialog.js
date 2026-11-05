// V2 drawer/flyup cadence; desktop modality belongs to the Schedule shell.
const desktop=matchMedia('(min-width:601px)');
const localDialogs=new Map();
desktop.addEventListener('change',()=>{document.querySelectorAll('#rings-surface[open],#v2-trainer-dialog[open]').forEach(dialog=>dialog.close());});
export function openShellDialog(dialog){
 if(dialog.open)return;
 if(!desktop.matches){dialog.showModal();return;}
 for(const opened of localDialogs.keys())opened.close();
 const shell=document.querySelector('.rings-shell');
 shell.append(dialog);dialog.dataset.shellLocal='true';
 const shade=document.createElement('div');shade.className='rings-dialog-shade';shade.setAttribute('aria-hidden','true');
 shade.addEventListener('click',()=>dialog.close());shell.insertBefore(shade,dialog);
 const siblings=[...shell.children].filter(node=>node!==dialog&&node!==shade).map(node=>[node,node.inert]);
 siblings.forEach(([node])=>node.inert=true);
 const escape=event=>{if(event.key==='Escape'){event.preventDefault();dialog.close();}};
 dialog.addEventListener('keydown',escape);
 localDialogs.set(dialog,shade);
 dialog.addEventListener('close',()=>{shade.remove();siblings.forEach(([node,inert])=>node.inert=inert);dialog.removeEventListener('keydown',escape);localDialogs.delete(dialog);},{once:true,capture:true});
 dialog.show();
}
