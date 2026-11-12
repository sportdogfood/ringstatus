import {sourceHandler} from './candidate.mjs';
const records=new Map();
export default {fetch(request,env){return sourceHandler.fetch(request,{...env,SGL_LANE_STORAGE:{get:key=>records.get(key),put:(key,value)=>records.set(key,value)}});}};
