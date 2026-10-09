import { env } from 'cloudflare:workers';
import { handleNativeRecognition } from '../../lib/rs-recognition-native.js';

// Explicitly authorized recognition-only entry within the existing cookie path.
export const ALL = ({ request }) => handleNativeRecognition(request, env);
