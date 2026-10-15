export const config = { runtime: "edge" };


const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-RS-Audit-Outcome"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: cors });

// The current native and API-agent interfaces use the shared Inputs credential
// controller. The historical public mutation route must not bypass that policy.
// Internal authenticated adapters still import the existing action implementation.
export const POST = async () => json({ ok: false, error: 'use_recognition_entry' }, 410);

function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { ...cors, "Content-Type": "application/json; charset=utf-8" } });
}
