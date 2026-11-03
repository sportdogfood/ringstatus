import client from '../../assets/rs-profile/optins.js?raw';
import {subscriptionAlertDefinitions} from '../../lib/rs-input-subscription-alerts.js';
export function GET(){
 const source=client.replace("import {definitions} from './optins-catalog.js';",'const definitions='+JSON.stringify(subscriptionAlertDefinitions)+';');
 return new Response(source,{headers:{'Content-Type':'text/javascript; charset=utf-8','Cache-Control':'no-store'}});
}
