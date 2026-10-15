import { createPersistence } from '../../assets/rs-inputs/persistence.js';
export type Kind = 'barn' | 'users' | 'riders' | 'horses' | 'locations';
export type Item = { id: string; barnId: string; name: string; email?: string; userId?: string; riderId?: string; locationId?: string; address?: string; recognitionPersonId?: string; revision?: number };
export type Store = { version: 1; barns: Item[]; users: Item[]; riders: Item[]; horses: Item[]; locations: Item[]; selectedBarn: string };
export type Draft = { id?: string; name: string; email: string; userId: string; riderId: string; locationId: string; address: string; revision?: number };
export type Profile = { person_uid: string; person_name: string; first_name: string; last_name: string; sms: string; pin: string; email: string };
export type RecognitionResult = { ok: true; recognized: boolean; profile: Profile | null; device: 'active' | 'unknown' | 'retired' };
export type RecognitionAction = 'update_profile' | 'confirm_device' | 'retire_device' | 'phone_login' | 'recovery';
export type AccessActor = { id: string; profile: { personUid: string; name: string; email: string } };
export const emptyStore = (): Store => ({ version: 1, barns: [], users: [], riders: [], horses: [], locations: [], selectedBarn: '' });
export const emptyProfile = (): Profile => ({ person_uid: '', person_name: '', first_name: '', last_name: '', sms: '', pin: '', email: '' });

const errorMessages: Record<string, string> = {
  authentication_required: 'Your access could not be verified. No changes were saved.',
  tenant_save_pending: 'Your save is queued. Your entry is kept; use Save again to check the same request.',
  tenant_save_needs_review: 'This save needs review. Your entry is kept; it has not been reported as saved.',
  save_barn_first: 'Save the barn first, then save its riders.',
  record_changed: 'This record has changed. Your entry is kept here. Loading the latest saved record will replace this unsaved entry; nothing is saved automatically.',
  duplicate_name: 'That name already exists in this barn. Open the existing record to edit it.',
  permission_denied: 'You do not have permission to make this change.',
  barn_not_found: 'This barn is not available to your account.',
  storage_unavailable: 'Records are temporarily unavailable. Your entry is still here; try again.',
  write_outcome_unknown: 'The save could not be confirmed. Your entry is still here; retry the same save to check its result.',
  invalid_relationship: 'A selected linked record is unavailable in this barn. Check your selections.',
  clean_input_base_required: 'The connected test base has not been configured.',
  recovery_delivery_unconfigured: 'Profile recovery is not available yet. Your details have been kept.'
};
export class InputApiError extends Error {
  constructor(public code: string) { super(errorMessages[code] || 'The request could not be completed. Your entry is still here; try again.'); }
}

function clearRecognitionCache() {
  sessionStorage.removeItem('rs_recognition_result');
  sessionStorage.removeItem('rs_recognition_welcome_shown');
  localStorage.removeItem('rs_recognition_button_state');
}

// Same token key, precedence and lifecycle as assets/rs-recognition/client.js.
export function deviceToken(create = false): string {
  const stored = localStorage.getItem('rs_device_token');
  const cookie = document.cookie.split('; ').find(part => part.startsWith('rs_device_token='));
  let fromCookie = '';
  try { fromCookie = cookie ? decodeURIComponent(cookie.slice('rs_device_token='.length)) : ''; } catch { /* malformed cookie is not an identity */ }
  let value = stored || fromCookie;
  if (value && !stored) localStorage.setItem('rs_device_token', value);
  if (!value && create) {
    value = `device_token_${crypto.randomUUID().replace(/-/g, '')}`;
    clearRecognitionCache();
    localStorage.setItem('rs_device_token', value);
    document.cookie = `rs_device_token=${encodeURIComponent(value)}; Max-Age=31536000; Path=/; SameSite=Lax; Secure`;
  }
  return value || '';
}

function clearDeviceToken() {
  localStorage.removeItem('rs_device_token');
  document.cookie = 'rs_device_token=; Max-Age=0; Path=/; SameSite=Lax; Secure';
  clearRecognitionCache();
}

function validStore(value: unknown): value is Store {
  if (!value || typeof value !== 'object') return false;
  const store = value as Store;
  return store.version === 1 && typeof store.selectedBarn === 'string' &&
    (['barns', 'users', 'riders', 'horses', 'locations'] as const).every(key =>
      Array.isArray(store[key]) && store[key].every(item => item && typeof item.id === 'string' && typeof item.name === 'string' && typeof item.barnId === 'string'));
}

function validRecognition(value: unknown): value is RecognitionResult {
  if (!value || typeof value !== 'object') return false;
  const result = value as RecognitionResult;
  return typeof result.recognized === 'boolean' && ['active', 'unknown', 'retired'].includes(result.device) &&
    (result.profile === null ? !result.recognized : Object.keys(emptyProfile()).every(key => typeof result.profile?.[key as keyof Profile] === 'string')) &&
    (!result.recognized || (result.device === 'active' && !!result.profile?.person_uid));
}

export function createInputsApi(apiBase: string, options: { durableBarn?: boolean; storage?: {read: Function; mutate: Function} } = {}) {
  const base = apiBase.replace(/\/$/, '');
  const requests = new Map<string, { fingerprint: string; id: string }>();
  let verifiedActor: AccessActor | null = null;
  const persistence=(kind: string)=>createPersistence({scope:{actorId:verifiedActor!.id,resource:`native-${kind}`,tenantId:'editor'},...(options.storage?{storage:options.storage}:{})});
  async function request(path: string, body?: Record<string, unknown>) {
    // Direct Barn/Rider saves await verified data, matching the WEC/HPS action contract.
    const directBarnSave = options.durableBarn && path === '/record' && ['barn','riders'].includes(String(body?.kind));
    const response = await fetch(`${base}${path}`, {
      method: body ? 'POST' : 'GET', credentials: 'same-origin', cache: 'no-store',
      ...(!directBarnSave && typeof AbortSignal!=='undefined'?{signal:AbortSignal.timeout(15000)}:{}),
      headers: { ...(body ? {'Content-Type':'application/json'} : {}), ...(options.durableBarn ? {'X-RS-Tenants':'barn-rider-v1', ...(verifiedActor ? {'X-RS-Actor':verifiedActor.id} : {})} : {}) },
      ...(body ? {body:JSON.stringify(body)} : {})
    }).catch(() => { throw new Error('Could not connect. Your entry is still here; try again.'); });
    const result = await response.json().catch(() => null);
    if (!response.ok || result?.ok !== true) throw new InputApiError(typeof result?.error === 'string' ? result.error : 'request_failed');
    return result;
  }
  async function mutation(path: string, body: Record<string, unknown>, validate: (result: any) => boolean) {
    const fingerprint = JSON.stringify(body);
    let pending = requests.get(path);
    if (!pending || pending.fingerprint !== fingerprint) {
      pending = { fingerprint, id: crypto.randomUUID() };
      requests.set(path, pending);
    }
    let device: any=null, operation: any=null;
    if(options.durableBarn&&path==='/record'&&['barn','riders'].includes(String(body.kind))){
      if(!verifiedActor)throw new InputApiError('authentication_required');
      device=persistence(String(body.kind));
      const saved=await device.load();
      if(saved.pending){
        const {requestId,...previous}=saved.pending.body;
        if(JSON.stringify(previous)!==JSON.stringify(body))throw new Error('An earlier save is still unresolved. Check that save before submitting a different entry.');
        pending.id=requestId;operation=saved.pending;
      }else{await device.edit(body);operation=await device.prepare({...body,requestId:pending.id});}
    }
    try{
      if(device){const current=accessActor(await request('/access'));if(current.id!==verifiedActor!.id)throw new InputApiError('authentication_required');}
      let result: any;
      for(let attempt=0;;attempt++){
        try{result=await request(path,{...body,requestId:pending.id});break;}
        catch(error){if(!(error instanceof InputApiError)||error.code!=='tenant_save_pending'||attempt>=12)throw error;await new Promise(resolve=>setTimeout(resolve,2000));}
      }
      if (!validate(result)) throw new Error('The server response could not be verified. Your entry is still here.');
      if(device)await device.confirm(operation,result.record,result.record.revision);
      if(options.durableBarn&&verifiedActor&&validStore(result.state))await persistence('state').refresh(result.state,0).catch(()=>{});
      requests.delete(path);return result;
    }catch(error){if(device)await device.fail(operation,error instanceof Error?error.message:'save_unconfirmed');throw error;}
  }
  const recordResponse = (result: any) => typeof result?.record?.id === 'string' && validStore(result.state);
  function accessActor(result: any): AccessActor {
    const actor = result?.actor;
    if (!actor || typeof actor.id !== 'string' || !actor.id ||
      typeof actor.profile?.personUid !== 'string' || !actor.profile.personUid ||
      typeof actor.profile.name !== 'string' || typeof actor.profile.email !== 'string') {
      throw new Error('Your access could not be verified. Please try again.');
    }
    return actor;
  }
  return {
    async access(): Promise<AccessActor> { const current=accessActor(await request('/access'));if(verifiedActor&&current.id!==verifiedActor.id)throw new InputApiError('authentication_required');verifiedActor=current;return current; },
    async cachedState(){if(!options.durableBarn||!verifiedActor)return null;try{const saved=(await persistence('state').load()).confirmed?.data;return validStore(saved)?saved:null;}catch{return null;}},
    async pendingDraft(kind: string){if(!options.durableBarn||!verifiedActor)return null;return (await persistence(kind).load()).pending?.body||null;},
    async acceptInvitation(token: string): Promise<AccessActor> {
      if (!/^[A-Za-z0-9_-]{43}$/.test(token)) throw new InputApiError('invalid_invitation');
      clearDeviceToken();
      return accessActor(await request('/access', { token }));
    },
    async logout(): Promise<void> { clearDeviceToken(); await request('/logout', {}); },
    resetDraftRequest() { requests.delete('/record'); },
    resetRecognitionRequest() { requests.delete('/recognition'); },
    async state(barnId = ''): Promise<Store> {
      const result = await request(`/state${barnId ? `?barn_id=${encodeURIComponent(barnId)}` : ''}`);
      if (!validStore(result.state)) throw new Error('The server returned an invalid record state.');
      if(options.durableBarn&&verifiedActor)await persistence('state').refresh(result.state,0).catch(()=>{});
      return result.state;
    },
    async record(kind: Kind, draft: Draft, barnId: string): Promise<{ record: Item; state: Store }> {
      return mutation('/record', { kind, draft, barnId, ...(draft.id && draft.revision !== undefined ? { expectedRevision: draft.revision } : {}) }, recordResponse);
    },
    async profileLink(barnId: string): Promise<{ record: Item; state: Store }> {
      return mutation('/profile-link', { barnId }, recordResponse);
    },
    async recognition(): Promise<RecognitionResult> {
      const result = await request(`/recognition?device_token=${encodeURIComponent(deviceToken())}`);
      if (!validRecognition(result)) throw new Error('The server returned an invalid recognition result.');
      return result;
    },
    async recognitionAction(action: RecognitionAction, values: Record<string, string>): Promise<RecognitionResult | { ok: true; accepted: true }> {
      const result = await mutation('/recognition', { action, values, device_token: deviceToken(action !== 'retire_device') }, value => action === 'recovery' ? value?.accepted === true : validRecognition(value));
      if (action === 'retire_device') clearDeviceToken();
      else if (action !== 'recovery') clearRecognitionCache();
      return result;
    }
  };
}
