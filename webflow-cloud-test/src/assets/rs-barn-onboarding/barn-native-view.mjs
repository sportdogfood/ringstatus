import { bindMainForms, inspectMainForms, FORMS, readDraft, fillDraft } from './barn-native-adapter.mjs';

const kinds = ['barn','users','riders','horses','locations'];
const mountedViews = new WeakMap();
const one = (root, selector) => {
  const nodes = root.querySelectorAll(selector);
  if (nodes.length !== 1) throw new Error(`native_target_mismatch:${selector}`);
  return nodes[0];
};
const errors = {
  name_required:'Enter a name.', barn_required:'Add a barn first.', invalid_email:'Enter a valid email address or leave it blank.',
  invalid_relationship:'Choose a linked record in this barn.', authentication_required:'Your access could not be verified. No changes were saved.',
  record_changed:'This record has changed. Your entry is kept here. Reopen the record to load its latest saved values.',
  write_outcome_unknown:'The save could not be confirmed. Your entry is still here; retry the same save to check its result.',
  duplicate_name:'That name already exists in this barn. Open the existing record to edit it.',
  permission_denied:'You do not have permission to make this change.',
  storage_unavailable:'Records are temporarily unavailable. Your entry is still here; try again.'
  ,chooser_template_missing:'The barn chooser is not configured on this page yet. Your entry is still here.'
};
const describeError = code => errors[code] || 'The request could not be completed. Your entry is still here; try again.';
// This functional class is defined natively in Webflow; no presentation is injected.
const show = (node, visible) => { node.hidden = !visible; node.classList.toggle('rs-barn-v24-state-hidden', !visible); };
const rowsFor = (state, kind) => kind === 'barn' ? state.barns : state[kind].filter(r => r.barnId === state.selectedBarn);

export function prepareNativeBarn({document, api, chooser}) {
  const {root,forms} = inspectMainForms(document);
  if (mountedViews.has(root)) return mountedViews.get(root);
  const panes = Object.fromEntries(kinds.map(kind=>[kind,forms[kind].form.closest('.rs-barn-v24-section')]));
  if (Object.values(panes).some(n=>!n)) throw new Error('native_pane_missing');
  const tabs = [...root.querySelectorAll('.rs-barn-v24-tab')];
  if (tabs.length !== 5) throw new Error('native_tabs_mismatch');
  const controls = Object.fromEntries(kinds.map(kind=>[kind,{
    error:one(panes[kind],'.rs-barn-v24-error'), success:one(panes[kind],'.rs-barn-v24-success'),
    empty:one(panes[kind],'.rs-barn-v24-empty'), edit:one(panes[kind],'.rs-barn-v24-edit'),
    add:one(panes[kind],'.rs-barn-v24-add-button'), cancel:one(forms[kind].form,'.rs-barn-v24-edit .rs-barn-v24-link'),
    note:one(panes[kind],'.rs-barn-v24-note')
  }]));
  const templates = Object.fromEntries(kinds.slice(1).map(kind=>{
    const node=one(panes[kind],'.rs-barn-v24-row');return [kind,{node,parent:node.parentElement,template:node.cloneNode(true)}];
  }));
  const profile=one(panes.users,':scope > .rs-barn-v24-link');
  const barnName=one(root,'.rs-barn-v24-barn-name');
  const chooseControl=one(root,'[aria-label="Choose barn"]');
  const editBarn=one(root,'[aria-label="Edit barn"]');
  const chooserSheet=one(root,'#rs-barn-v24-chooser');
  const originalChooserOption=one(chooserSheet,'#rs-barn-v24-chooser-option');
  chooser ||= {container:one(chooserSheet,'#rs-barn-v24-chooser-options'),optionTemplate:one(chooserSheet,'#rs-barn-v24-chooser-option').cloneNode(true)};
  if(!root.contains(chooser.container)||chooser.container===chooser.optionTemplate)throw new Error('native_chooser_target_mismatch');
  const sheetDefs={
    barn:{id:'rs-barn-v24-sheet-barn',kind:'barn',fields:{name:'input[name="Barn name"]'}},
    users:{id:'rs-barn-v24-sheet-user-nested',kind:'users',fields:{name:'input[name="User name"]',email:'input[name="Email (optional)"]'}},
    riders:{id:'rs-barn-v24-sheet-rider',kind:'riders',fields:{name:'input[name="Rider name"]',userId:'#rs-barn-v24-sheet-user-select'}},
    locations:{id:'rs-barn-v24-sheet-location',kind:'locations',fields:{name:'input[name="Location name"]',address:'input[name="Address or description (optional)"]'}}
  };
  const sheets=Object.fromEntries(Object.entries(sheetDefs).map(([kind,spec])=>{
    const element=one(root,'#'+spec.id);
    return [kind,{...spec,element,form:one(element,'.rs-barn-v24-sheet-form'),fields:Object.fromEntries(Object.entries(spec.fields).map(([f,s])=>[f,one(element,s)])),
      save:one(element,'.rs-barn-v24-primary'),close:one(element,'.rs-barn-v24-sheet-close'),description:one(element,'.rs-barn-v24-sheet-description'),
      originalDescription:one(element,'.rs-barn-v24-sheet-description').textContent}];
  }));
  const optionTemplate=one(sheets.riders.element,'option[value=""]').cloneNode(true);
  const originalOptions = new Map([...Object.values(forms).flatMap(f=>Object.values(f.fields)),sheets.riders.fields.userId].filter(n=>n.tagName==='SELECT').map(n=>[n,[...n.childNodes].map(c=>c.cloneNode(true))]));
  const removes=[]; let controller,frames=[],busy=false,currentKind='barn',disposed=false,pendingFocus=null;
  const listen=(node,type,fn)=>{node.addEventListener(type,fn,true);removes.push(()=>node.removeEventListener(type,fn,true));};
  const click=(node,fn)=>{const handle=event=>{event.preventDefault();event.stopImmediatePropagation();if(!busy&&!disposed)void fn(node);};listen(node,'click',handle);
    if(node.tagName==='A'){node.setAttribute('role','button');node.setAttribute('tabindex','0');listen(node,'keydown',event=>{if(event.key===' '||event.key==='Enter')handle(event);});}};
  function clearFeedback(kind){show(controls[kind].error,false);show(controls[kind].success,false);}
  function selectTab(kind){currentKind=kind;tabs[kinds.indexOf(kind)].click();}
  function renderOptions(select, records) {
    const previous=select.value;
    const placeholder=optionTemplate.cloneNode(true);placeholder.textContent='Choose';placeholder.value='';
    const options=records.map(record=>{const node=optionTemplate.cloneNode(true);node.value=record.id;node.textContent=record.name;return node;});
    select.replaceChildren(placeholder,...options);
    select.value=records.some(record=>record.id===previous)?previous:'';
  }
  function cleanClone(template){const row=template.cloneNode(true);for(const node of [row,...row.querySelectorAll('[id],[data-w-id]')]){node.removeAttribute('id');node.removeAttribute('data-w-id');}return row;}
  function renderRows(state,kind){
    const slot=templates[kind];const rows=rowsFor(state,kind).map(record=>{
      const row=cleanClone(slot.template);row.dataset.rsRecordId=record.id;
      one(row,'.rs-barn-v24-row-name').textContent=record.name;
      one(row,'.rs-barn-v24-edit-button').setAttribute('aria-label',`Edit ${record.name}`);
      for(const action of row.querySelectorAll('a')){action.setAttribute('role','button');action.setAttribute('tabindex','0');}
      if(kind==='riders'){
        const user=rowsFor(state,'users').find(r=>r.id===record.userId);
        const target=one(row,'.rs-barn-v24-help');target.textContent=user?.name||'';show(target,!!user);
      }
      if(kind==='horses'){
        const pills=[...row.querySelectorAll('.rs-barn-v24-pill')];
        for(const [i,field,related]of [[0,'riderId','riders'],[1,'locationId','locations']]){
          const linked=rowsFor(state,related).find(r=>r.id===record[field]);
          pills[i].textContent=linked?.name||'';show(pills[i],!!linked);pills[i].dataset.rsRelatedKind=related;pills[i].dataset.rsRelatedId=linked?.id||'';
        }
      }
      return row;
    });
    for(const old of [...slot.parent.children]) if(old.classList.contains('rs-barn-v24-row'))old.remove();
    slot.parent.append(...rows);
  }
  function restoreFocus(node){if(node?.isConnected)node.focus();}
  function showSheet(frame){const sheet=sheets[frame.kind];sheet.description.textContent=sheet.originalDescription;sheet.description.setAttribute('role','status');
    sheet.element.showPopover();Object.values(sheet.fields)[0].focus();}
  function closeSheets(){if(busy)return;const first=frames[0];for(const f of frames)if(sheets[f.kind].element.matches(':popover-open'))sheets[f.kind].element.hidePopover();frames=[];restoreFocus(first?.opener);}
  function backSheet(){if(busy)return;const frame=frames.pop();if(!frame)return;if(sheets[frame.kind].element.matches(':popover-open'))sheets[frame.kind].element.hidePopover();if(frames.length){showSheet(frames.at(-1));restoreFocus(frame.opener);}else restoreFocus(frame.opener);}
  function openSheet(kind,{field,ownerKind,opener,nested=false}={}){
    if(busy||!controller.getState())return;
    if(!nested)closeSheets();
    fillDraft(sheets[kind].fields);
    frames.push({kind,field,ownerKind,opener});
    const back=sheets.users.element.querySelector('.rs-barn-v24-back');if(kind==='users')show(back,nested);
    showSheet(frames.at(-1));
  }
  async function saveSheet(kind){
    const frame=frames.at(-1);if(!frame||frame.kind!==kind||busy)return;
    const sheet=sheets[kind];const draft=readDraft(sheet.fields);
    const result=await controller.saveLinked(kind,draft);if(!result||disposed)return;
    frames.pop();if(sheet.element.matches(':popover-open'))sheet.element.hidePopover();
    if(frames.length){const previous=frames.at(-1);if(frame.field)sheets[previous.kind].fields[frame.field].value=result.record.id;showSheet(previous);restoreFocus(frame.opener);}
    else {if(frame.field&&frame.ownerKind)forms[frame.ownerKind].fields[frame.field].value=result.record.id;if(kind==='barn')selectTab('users');restoreFocus(frame.opener);}
  }
  const view={
    state(state,actor){
      barnName.textContent=state.barns.find(b=>b.id===state.selectedBarn)?.name||'Choose barn';
      profile.textContent=`Use my recognized profile${actor?.profile?.name?' · '+actor.profile.name:''}`;
      for(const kind of kinds){show(controls[kind].empty,rowsFor(state,kind).length===0);controls[kind].note.textContent='Connected records';if(kind!=='barn')renderRows(state,kind);}
      renderOptions(forms.riders.fields.userId,rowsFor(state,'users'));renderOptions(sheets.riders.fields.userId,rowsFor(state,'users'));
      renderOptions(forms.horses.fields.riderId,rowsFor(state,'riders'));renderOptions(forms.horses.fields.locationId,rowsFor(state,'locations'));
      if(chooser)renderChooser(state);
    },
    feedback({kind,type,value}){
      if(frames.length&&type==='error'){const sheet=sheets[frames.at(-1).kind];sheet.description.textContent=describeError(value);sheet.description.setAttribute('role','alert');if(!sheet.element.matches(':popover-open'))sheet.element.showPopover();return;}
      clearFeedback(kind);const target=controls[kind][type];target.textContent=type==='error'?describeError(value):value;target.setAttribute('role',type==='error'?'alert':'status');show(target,true);
    },
    busy(value){busy=value;root.setAttribute('aria-busy',String(value));root.inert=value;for(const form of Object.values(forms))form.form.inert=value;for(const sheet of Object.values(sheets))sheet.form.inert=value;if(!value&&pendingFocus){pendingFocus.focus();pendingFocus=null;}},
    editor({kind,record,open}){selectTab(kind);show(forms[kind].form,open);show(controls[kind].edit,open&&!!record);show(controls[kind].add,!open);clearFeedback(kind);
      const title=controls[kind].edit.firstElementChild;if(title&&record)title.textContent=`Editing ${record.name}`;if(open){if(busy)pendingFocus=forms[kind].fields.name;else forms[kind].fields.name.focus();}}
  };
  controller=bindMainForms({document,api,view});
  for(const kind of kinds){clearFeedback(kind);show(controls[kind].edit,false);}
  for(const kind of kinds){
    click(controls[kind].add,node=>kind==='barn'?openSheet('barn',{opener:node}):controller.add(kind));
    click(controls[kind].cancel,()=>controller.cancel(kind));
    for(const field of Object.values(forms[kind].fields))listen(field,'input',()=>clearFeedback(kind));
    if(kind!=='barn'){const activateRow=event=>{
      if(event.type==='keydown'&&!['Enter',' '].includes(event.key))return;
      const action=event.target.closest('.rs-barn-v24-row-name,.rs-barn-v24-edit-button,.rs-barn-v24-pill');
      if(!action||!templates[kind].parent.contains(action))return;event.preventDefault();event.stopImmediatePropagation();if(busy)return;
      const row=action.closest('.rs-barn-v24-row');if(!row)return;
      void controller.beginEdit(action.dataset.rsRelatedKind||kind,action.dataset.rsRelatedId||row.dataset.rsRecordId);
    };listen(templates[kind].parent,'click',activateRow);listen(templates[kind].parent,'keydown',activateRow);}
  }
  click(profile,()=>controller.profileLink());click(editBarn,()=>{const state=controller.getState();if(state?.selectedBarn)return controller.beginEdit('barn',state.selectedBarn);});
  for(const [field,ownerKind,kind]of [['userId','riders','users'],['riderId','horses','riders'],['locationId','horses','locations']]){
    click(one(forms[ownerKind].fields[field].parentElement,'button.rs-barn-v24-link[popovertarget]'),node=>openSheet(kind,{field,ownerKind,opener:node}));
  }
  click(one(sheets.riders.element,'[popovertarget="rs-barn-v24-sheet-user-nested"]'),node=>openSheet('users',{field:'userId',opener:node,nested:true}));
  click(one(sheets.users.element,'.rs-barn-v24-back'),()=>backSheet());
  for(const [kind,sheet]of Object.entries(sheets)){
    click(sheet.save,()=>saveSheet(kind));click(sheet.close,()=>closeSheets());
    listen(sheet.form,'submit',event=>{event.preventDefault();event.stopImmediatePropagation();void saveSheet(kind);});
    listen(sheet.element,'keydown',event=>{if(event.key==='Escape'){event.preventDefault();event.stopImmediatePropagation();closeSheets();}});
    listen(sheet.element,'toggle',event=>{if(event.newState==='closed'&&frames.at(-1)?.kind===kind&&!busy&&!sheet.element.matches(':popover-open'))closeSheets();});
    click(one(sheet.element,'.rs-barn-v24-sheet-overlay'),()=>closeSheets());
  }
  // Clone the designated native option, preserving its presentation.
  function renderChooser(state){
    const nodes=state.barns.map(record=>{const option=cleanClone(chooser.optionTemplate);option.textContent=record.name;option.dataset.rsBarnId=record.id;option.setAttribute('aria-current',String(record.id===state.selectedBarn));option.setAttribute('role','button');option.setAttribute('tabindex','0');return option;});
    chooser.container.replaceChildren(...nodes);
  }
  const closeChooser=()=>{if(chooserSheet.matches(':popover-open'))chooserSheet.hidePopover();restoreFocus(chooseControl);};
  const openChooser=()=>{closeSheets();chooserSheet.showPopover();chooser.container.querySelector('[data-rs-barn-id]')?.focus();};
  click(chooseControl,openChooser);click(barnName,openChooser);
  click(one(chooserSheet,'.rs-barn-v24-sheet-close'),closeChooser);
  click(one(chooserSheet,'.rs-barn-v24-sheet-overlay'),closeChooser);
  click(one(chooserSheet,'.rs-barn-v24-primary'),()=>{closeChooser();openSheet('barn',{opener:chooseControl});});
  const selectOption=event=>{
    if(event.type==='keydown'&&!['Enter',' '].includes(event.key))return;
    const option=event.target.closest('[data-rs-barn-id]');if(!option||busy)return;
    event.preventDefault();event.stopImmediatePropagation();
    // Close first so an access or state error is visible on the main native pane.
    closeChooser();void controller.selectBarn(option.dataset.rsBarnId);
  };
  listen(chooser.container,'click',selectOption);listen(chooser.container,'keydown',selectOption);
  listen(chooserSheet,'keydown',event=>{if(event.key==='Escape'){event.preventDefault();event.stopImmediatePropagation();closeChooser();}});
  const result={...controller,openSheet,closeSheets,backSheet,
    dispose(){disposed=true;removes.forEach(fn=>fn());controller.dispose();root.inert=false;root.removeAttribute('aria-busy');for(const f of Object.values(forms))f.form.inert=false;for(const s of Object.values(sheets))s.form.inert=false;
      for(const slot of Object.values(templates)){for(const row of [...slot.parent.children])if(row.classList.contains('rs-barn-v24-row'))row.remove();slot.parent.append(slot.node);}
      for(const [select,options]of originalOptions)select.replaceChildren(...options);
      chooser.container.replaceChildren(originalChooserOption);
      mountedViews.delete(root);}};
  mountedViews.set(root,result);return result;
}
