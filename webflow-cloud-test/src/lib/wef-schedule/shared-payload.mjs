import { Buffer } from 'node:buffer';
import { gzipSync, gunzipSync } from 'node:zlib';

const FIELD_LIMIT = 100000;
const ENCODING = 'gzip-base64-v1';

// Only the saved representation changes. Astro and the device still receive the full JSON.
export function encodePayload(data) {
  const json = JSON.stringify(data);
  const text = json.length <= FIELD_LIMIT ? json : JSON.stringify({
    encoding:ENCODING, data:gzipSync(json).toString('base64')
  });
  if (text.length > FIELD_LIMIT) throw Error(`schedule_feed_payload_too_large:${data.metadata.defaultDate}`);
  return text;
}

export function decodePayload(text) {
  const value = JSON.parse(text);
  if (!value?.encoding) return value;
  if (value.encoding !== ENCODING || typeof value.data !== 'string' ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.data)) {
    throw Error('invalid_schedule_feed_encoding');
  }
  return JSON.parse(gunzipSync(Buffer.from(value.data,'base64')).toString('utf8'));
}
