import { runScheduleRefresh } from './refresh-run.mjs';

export async function runDueFeeds({api,state,save,now=new Date(),refresh=runScheduleRefresh}) {
  const slot=new Date(Math.floor(now.getTime()/180000)*180000).toISOString();
  const outcomes=[];
  if(state.stopped)return {slot,stopped:state.stopped,outcomes};
  for(const [lane,interval] of [['day0',180000],['full',900000]]) {
    if(state[lane] && Date.parse(slot)-Date.parse(state[lane])<interval)continue;
    try {
      const receipt=await refresh(api,{lane,slot});
      outcomes.push({lane,...receipt});
      if(receipt.log.result!=='Success'||receipt.loggingErrors.length)throw Error('schedule_'+lane+'_failed');
      state[lane]=slot;
    } catch(error) {
      state.stopped={lane,slot,error:/^[a-z0-9_:-]+$/.test(error.message)?error.message:'schedule_refresh_failed'};
    }
    await save(state);
    if(state.stopped)break;
  }
  return {slot,stopped:state.stopped??null,outcomes};
}
