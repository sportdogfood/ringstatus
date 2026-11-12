import {v3Button} from '../rs-profile-v3/components.js';
import {icon} from '../rs-profile-v3/icons.js';
import {HORSE_COLORS,HORSE_GENDERS} from './horse-fields.js';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const nickname=(value,inline=false)=>'<label class="field"><span'+(inline?' class="sr-only"':' class="detail-label"')+'>Nickname</span><input name="nickname" aria-label="Horse nickname" value="'+esc(value)+'" maxlength="80" autocomplete="off"></label>';
export const inlineNickname=value=>'<form class="v2-nickname-form">'+nickname(value,true)+'<button type="submit" class="icon-button" aria-label="Save nickname">'+icon('check')+'</button><button type="button" class="icon-button" data-cancel-horse aria-label="Cancel nickname">'+icon('close')+'</button><span class="form-error" role="alert"></span></form>';
export function horseForm(fields){
 const choices=(name,label,items)=>'<fieldset><legend>'+label+'</legend><div class="horse-choice-row">'+items.map(value=>v3Button('horse-choice',esc(value),{extra:'data-horse-field="'+name+'" data-value="'+esc(value)+'" aria-pressed="'+(fields[name]===value)+'"'})).join('')+'</div><input type="hidden" name="'+name+'" value="'+esc(fields[name]||'')+'"></fieldset>';
 return '<form class="v2-horse-form"><div class="form-grid"><div class="full">'+nickname(fields.nickname)+'</div><div class="full">'+choices('gender','Gender',HORSE_GENDERS)+'</div><div class="full">'+choices('color','Color',HORSE_COLORS)+'</div></div><p class="form-error" role="alert"></p><div class="dialog-actions">'+v3Button('cancel-horse','Cancel',{extra:'data-cancel-horse'})+'<button class="ui-button" type="submit" data-variant="primary">Save</button></div></form>';
}
