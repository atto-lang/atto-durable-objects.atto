// Executes actual attoc-produced Wasm. No host implementation of the guest.
// Run with ATTOC=/absolute/path/to/pinned/compiler node tests/durable_counter_guest.mjs
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
assert.ok(process.env.ATTOC, 'ATTOC must identify a real built compiler');
const compiler = resolve(process.env.ATTOC);
assert.ok(existsSync(compiler), 'compiler missing: ' + compiler);
const source = readFileSync(new URL('../guest/durable_counter.atto', import.meta.url), 'utf8');
const dir = mkdtempSync(join(tmpdir(), 'atto-durable-guest-'));
const results = [];
const build = (name, text) => {
    const input = join(dir, name + '.atto');
    const output = join(dir, name + '.wasm');
    writeFileSync(input, text);
    rmSync(output, { force: true });
    const run = spawnSync(compiler, [input, '-o', output], {
        cwd: dir, encoding: 'utf8', timeout: 30000, maxBuffer: 1 << 20
    });
    assert.equal(run.error, undefined, name + ': compiler invocation failed');
    return {run, output};
};
function execute(name, body, expected) {
    const text = source + '\npub fnc main() -> int { ' + body + ' }\n';
    const {run, output} = build(name, text);
    assert.equal(run.status, 0, name + ': ' + run.stderr);
    assert.ok(existsSync(output), name + ': no artifact');
    const bytes = readFileSync(output);
    assert.ok(WebAssembly.validate(bytes), name + ': invalid Wasm');
    const mod = new WebAssembly.Module(bytes);
    assert.deepEqual(WebAssembly.Module.imports(mod), [], 'guest must not access host authority');
    const inst = new WebAssembly.Instance(mod, {});
    assert.equal(typeof inst.exports.main, 'function');
    assert.equal(inst.exports.main(), expected, name + ': wrong result');
    assert.equal(inst.exports.panic_code.value, 0, name + ': unexpected panic');
    results.push({case: name, source_sha256: hash(text), wasm_sha256: hash(bytes),
        result: String(expected), guest_executed: true});
}
try {
    execute('self_test', 'return durable_counter_self_test();', 0n);
    execute('maximum', 'return durable_counter_next(999999999, 1, 1);', 1000000000n);
    execute('overflow', 'return durable_counter_next(1000000000, 1, 1);', -1n);
    execute('invalid_operation', 'return durable_counter_next(1, 3, 1);', -1n);
    execute('negative_delta', 'return durable_counter_next(1, 1, -1);', -1n);
    const invalid = source + '\npub fnc main() -> int { return durable_counter_next(true, 1, 1); }\n';
    const {run, output} = build('invalid_type', invalid);
    assert.equal(run.status, 1, 'wrongly typed guest must be rejected');
    assert.ok(run.stderr.trim(), 'expected compiler diagnostic');
    assert.equal(existsSync(output), false, 'negative compile produced artifact');
    results.push({case:'invalid_type', source_sha256:hash(invalid), diagnostic:run.stderr.trim(), no_artifact:true});
    console.log(JSON.stringify({kind:'SCALAR_GUEST_ONLY', fixture:fileURLToPath(new URL('../guest/durable_counter.atto', import.meta.url)),
      fixture_sha256:hash(source), compiler_binary_sha256:hash(readFileSync(compiler)),
      compiler_source_head_claim:process.env.ATTO_COMPILER_SOURCE_SHA ?? null, node:process.version, results}, null, 2));
} finally {
    rmSync(dir, {recursive:true, force:true});
}
