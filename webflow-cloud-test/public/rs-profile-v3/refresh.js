export const REFRESH_INTERVAL=8*60*1000;
// Browser-owned cadence; no work runs while the page is closed.
export function startProfileRefresh({refresh,ready,setTimer=setTimeout,clearTimer=clearTimeout}){
 let timer=null,stopped=false;
 const schedule=()=>{if(!stopped)timer=setTimer(tick,REFRESH_INTERVAL);};
 const tick=async()=>{timer=null;try{if(ready())await refresh();}catch{/* Existing service retains data and reports the failure. */}finally{schedule();}};
 schedule();return()=>{stopped=true;if(timer!==null)clearTimer(timer);};
}
