import {env} from 'cloudflare:workers';
import {handleProfile} from '../../lib/rs-profile.js';
export const ALL=({request,locals})=>handleProfile(request,env,fetch,locals?.cfContext);
