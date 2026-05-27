const assert = require("assert");

const {
  buildSglTokenFields,
  deriveSglTokenPrefix,
} = require("../sgl_token_utils");

const token = "Mjk2NjUzMjV2ariIyyCQZPDv7zbOun9FgTDI3QkcOx/cVS9pqXo5GQ0lNngLH21XXxq0SFQyQf31lxLEK+tt9or4yVi6+MyVXZ61a1n9ISsBo434LDaiy8AAV/Cz5Rqb4wkI8t1908kwnimmbY3v3oKpcv+ys2k1du22eE+SsjVU1HRBrkS+mzmmmEnl/CwvegmFf4fh31p15umFfpD5JuTEFKwr/cQ6tn51TQ==";

assert.strictEqual(deriveSglTokenPrefix(token), "29665325v");

const fields = buildSglTokenFields(token);
assert.strictEqual(fields.sgl_token_raw, token);
assert.strictEqual(fields.sgl_token_prefix, "29665325v");
assert.strictEqual(fields.sgl_token_length, token.length);
assert.strictEqual(fields.sgl_token_hash.length, 64);

assert.deepStrictEqual(buildSglTokenFields(null), {});
assert.deepStrictEqual(buildSglTokenFields(""), {});

console.log("sgl_token_utils tests passed");
