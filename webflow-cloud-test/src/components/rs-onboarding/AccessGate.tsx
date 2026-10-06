import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { createInputsApi, InputApiError, type AccessActor } from './api';
import { MobileScroll, useKeyboard } from './mobile';

type GateState = 'checking' | 'invitation' | 'required' | 'ready' | 'error' | 'accepting' | 'logging-out' | 'logout-error';

export default function AccessGate({ apiBase, children }: {
  apiBase: string;
  children: (actor: AccessActor, accessControl: ReactNode) => ReactNode;
}) {
  const api = useMemo(() => createInputsApi(apiBase), [apiBase]);
  const keyboard = useKeyboard();
  const invitation = useRef<string | null>(null);
  const captured = useRef(false);
  const busy = useRef(false);
  const [state, setState] = useState<GateState>('checking');
  const [actor, setActor] = useState<AccessActor | null>(null);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let current = true;
    if (!captured.current) {
      captured.current = true;
      const fragment = new URLSearchParams(window.location.hash.slice(1));
      if (fragment.has('invite')) {
        const tokens = fragment.getAll('invite');
        // Remove the secret from the address bar/history before any request.
        window.history.replaceState(window.history.state, '', window.location.pathname + window.location.search);
        invitation.current = tokens.length === 1 && /^[A-Za-z0-9_-]{43}$/.test(tokens[0]) ? tokens[0] : '';
      }
    }
    if (invitation.current !== null) {
      setState('invitation');
      if (!invitation.current) setError('This invitation link is incomplete or invalid. Ask for a new invitation.');
      return;
    }
    setState('checking');
    setError('');
    void api.access().then(value => {
      if (current) { setActor(value); setState('ready'); }
    }).catch(err => {
      if (!current) return;
      setActor(null);
      if (err instanceof InputApiError && err.code === 'authentication_required') setState('required');
      else { setError('Access could not be checked. Please try again.'); setState('error'); }
    });
    return () => { current = false; };
  }, [api, attempt]);

  async function acceptInvitation() {
    if (busy.current || !invitation.current) return;
    busy.current = true;
    setState('accepting');
    setError('');
    try {
      const value = await api.acceptInvitation(invitation.current);
      invitation.current = null;
      setActor(value);
      setState('ready');
    } catch {
      setError('The invitation could not be accepted. If a retry fails, ask for a new invitation: a lost response may have used this link.');
      setState('invitation');
    } finally { busy.current = false; }
  }

  async function logout() {
    if (busy.current) return;
    busy.current = true;
    keyboard.hide();
    // Unmount all records and drafts immediately, including when logout needs a retry.
    setActor(null);
    invitation.current = null;
    setState('logging-out');
    setError('');
    try { await api.logout(); setState('required'); }
    catch { setError('Sign out could not be confirmed. Please try again.'); setState('logout-error'); }
    finally { busy.current = false; }
  }

  if (state === 'ready' && actor) return children(actor,
    <button className="rings-view-switch" type="button" onClick={() => void logout()}>Sign out</button>);

  const pending = state === 'checking' || state === 'accepting' || state === 'logging-out';
  return <div className="rings-combined" data-theme="white" data-view="mobile">
    <MobileScroll className="app-screen rings-screen">
      <main className="rings-main" aria-busy={pending}>
        <div className="rings-wordmark">Ring<span>Status</span></div>
        <h1>{state === 'invitation' || state === 'accepting' ? 'Your invitation' : 'Welcome to RingStatus'}</h1>
        {pending ? <p className="rings-help" role="status">{state === 'checking' ? 'Checking access…' : state === 'accepting' ? 'Accepting invitation…' : 'Signing out…'}</p> : null}
        {error ? <p className="rings-error" role="alert">{error}</p> : null}
        {state === 'invitation' && invitation.current ? <>
          <p className="rings-help">Accept your invitation to open your RingStatus account.</p>
          <button className="rings-primary" type="button" onClick={() => void acceptInvitation()}>Accept invitation</button>
        </> : null}
        {state === 'required' ? <p className="rings-help">Open your invitation link to continue. If you need access, ask the person who invited you for a new link.</p> : null}
        {state === 'error' || state === 'required' ? <button className="rings-secondary" type="button" onClick={() => setAttempt(value => value + 1)}>Check access again</button> : null}
        {state === 'logout-error' ? <button className="rings-secondary" type="button" onClick={() => void logout()}>Try signing out again</button> : null}
      </main>
    </MobileScroll>
  </div>;
}
