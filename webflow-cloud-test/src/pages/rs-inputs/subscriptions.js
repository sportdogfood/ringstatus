import { env } from 'cloudflare:workers';
import { createSubscriptionHandler } from '../../lib/rs-input-subscriptions.js';
import { subscriptionAlertDefinitions } from '../../lib/rs-input-subscription-alerts.js';

// Existing Inputs auth and the same control DB binding. Missing subscription migration
// fails closed. Grant notice and engine catalog remain unconfigured, never inferred.
export const ALL = context => createSubscriptionHandler({
  env, definitions: subscriptionAlertDefinitions,
})(context.request);
