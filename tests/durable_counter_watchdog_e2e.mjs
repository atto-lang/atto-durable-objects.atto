import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
assert.ok(process.env.ATTOC && process.env.COUNTER_PROCESS_TEST);
const compiler=resolve(process.env.ATTOC), test=resolve(process.env.COUNTER_PROCESS_TEST);
const worker=fileURLToPath(new URL('../guest/counter_process_worker.mjs',import.meta.url));
const dir=mkdtempSync(join(tmpdir(),'atto-process-guest-'));
const hash=b=>createHash('sha256').update(b).digest('hex');
try {
    const source=readFileSync(new URL('../guest/durable_counter.atto',import.meta.url),'utf8')+
        '\nfnc durable_transition(int n,int op,int d)->int { while (d eq 7) { } return durable_counter_next(n,op,d); }\npub fnc main()->int { return 0; }\n';
    const path=join(dir,'counter.atto'), artifact=join(dir,'counter.wasm');
    writeFileSync(path,source);
    const build=spawnSync(compiler,['--durable-scalar-v1',path,'-o',artifact],{encoding:'utf8',timeout:30000,maxBuffer:1<<20});
    assert.equal(build.error,undefined);assert.equal(build.status,0,build.stderr);
    const bytes=readFileSync(artifact),digest=hash(bytes);
    const result=spawnSync(test,[process.execPath,worker,artifact,digest],{encoding:'utf8',timeout:60000,maxBuffer:1<<20});
    assert.equal(result.error,undefined);assert.equal(result.status,0,result.stderr);
    console.log(JSON.stringify({kind:'COMPILED_COUNTER_PROCESS_WATCHDOG_PASS',compiler_invocations:1,
      compiler_source_head_claim:process.env.ATTO_COMPILER_SOURCE_SHA??null,source_sha256:hash(source),
      wasm_sha256:digest,compiler_binary_sha256:hash(readFileSync(compiler)),
      worker_sha256:hash(readFileSync(worker)),driver_sha256:hash(readFileSync(test)),
      node:process.version,timeout_ms:1500,expected_reaped_timeout_children:4}));
    if(process.env.COUNTER_FUEL_WORKER) {
        const native=resolve(process.env.COUNTER_FUEL_WORKER);
        const metered=spawnSync(test,[native,artifact,digest],{encoding:'utf8',timeout:60000,maxBuffer:1<<20});
        assert.equal(metered.error,undefined);assert.equal(metered.status,0,metered.stderr);
        console.log(JSON.stringify({kind:'COMPILED_COUNTER_FUEL_WORKER_PASS',compiler_invocations:1,
          wasm_sha256:digest,source_sha256:hash(source),fuel_worker_sha256:hash(readFileSync(native)),
          driver_sha256:hash(readFileSync(test)),engine:'wasmi-0.31.2',fuel_budget:50000,
          address_space_bytes:268435456,cpu_seconds:2,expected_reaped_fuel_failures:4}));
    }
} finally {rmSync(dir,{recursive:true,force:true});}
