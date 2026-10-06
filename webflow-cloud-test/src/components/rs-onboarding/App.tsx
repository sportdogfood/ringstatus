import { MobileRuntime } from "./mobile";
import Prototype from "./Prototype";
import AccessGate from "./AccessGate";

export default function App({apiBase}:{apiBase?:string}) {
  return (
    <MobileRuntime>
      {apiBase ? <AccessGate key={apiBase} apiBase={apiBase}>
        {(actor, accessControl) => <Prototype key={actor.id} apiBase={apiBase} accessControl={accessControl} />}
      </AccessGate> : <Prototype />}
    </MobileRuntime>
  );
}
