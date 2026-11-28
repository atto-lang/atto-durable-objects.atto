// Real compiled scalar guest + canonical SQLite host store across OS processes.
// Test-only driver, sequential trusted scheduling, no remote ingress.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createInterface } from 'node:readline';

assert.ok(process.env.ATTOC && process.env.COUNTER_DRIVER, 'real compiler and host driver required');
const compiler=resolve(process.env.ATTOC), driver=resolve(process.env.COUNTER_DRIVER);
const actorProfile=process.env.DURABLE_ACTOR_PROFILE==='1';
const jsPath=process.env.DURABLE_JS_COMPILER;
const hybrid=Boolean(jsPath);
assert.ok(!hybrid || actorProfile,'hybrid requires real durable actor source');
let jsCompiled,backend='wasm';
const source=readFileSync(new URL(actorProfile?'../guest/durable_counter_actor.atto':'../guest/durable_counter.atto',import.meta.url),'utf8');
const dir=mkdtempSync(join(tmpdir(),'atto-counter-e2e-'));
const db=join(dir,'counter.db');
const hash=b=>createHash('sha256').update(b).digest('hex');
let executed=0;
const receipts=[];
// One source compilation, one reusable module, fresh instance per turn.
// The checked compiler's opt-in export carries three i64 values and returns i64.
const text=actorProfile?source:source+'\nfnc durable_transition(int n,int op,int d)->int { return durable_counter_next(n,op,d); }\npub fnc main()->int { return 0; }\n';
function compileOnce() {
    const input=join(dir,'counter.atto'), output=join(dir,'counter.wasm');
    writeFileSync(input,text); rmSync(output,{force:true});
    const r=spawnSync(compiler,[actorProfile?'--durable-counter-actor-v1':'--durable-scalar-v1',input,'-o',output],{cwd:dir,encoding:'utf8',timeout:30000,maxBuffer:1<<20});
    assert.equal(r.error,undefined); assert.equal(r.status,0,r.stderr);
    assert.ok(existsSync(output),'real compiler did not emit artifact');
    const bytes=readFileSync(output);
    assert.ok(WebAssembly.validate(bytes));
    const mod=new WebAssembly.Module(bytes);
    assert.deepEqual(WebAssembly.Module.imports(mod),[]);
    assert.deepEqual(WebAssembly.Module.exports(mod).map(x=>x.name).sort(),
        ['atto_durable_transition_v1','main','panic_code']);
    return {mod,sha:hash(bytes)};
}
let compiled;
async function compileJS() {
    const {compile_durable_counter_actor_v1}=await import(pathToFileURL(resolve(jsPath)));
    const result=compile_durable_counter_actor_v1(text);
    assert.equal(result.guest_abi,'atto.js.durable.counter.v1');
    const module=await import('data:text/javascript;base64,'+Buffer.from(result.module_source).toString('base64'));
    assert.deepEqual(Object.keys(module).sort(),['atto_durable_state_v1','atto_durable_transition_v1']);
    const sections=WebAssembly.Module.customSections(compiled.mod,'atto.durable.state.v1');
    assert.equal(sections.length,1);
    const descriptor=Buffer.from(sections[0]);
    assert.deepEqual(descriptor,Buffer.from(module.atto_durable_state_v1()));
    // The test-only broker's decimal storage uses a fixed Counter.count
    // profile; reject any schema not matching the admitted zero-state ABI.
    const snapshot=descriptor.subarray(17);
    assert.equal(descriptor.subarray(2,9).toString(),'Counter');
    assert.equal(descriptor.subarray(10,15).toString(),'count');
    assert.equal(snapshot.length,36);
    assert.equal(snapshot.readBigUInt64LE(9),0x46ec970556cc8237n);
    assert.equal(snapshot.readBigInt64LE(20),0n);
    assert.equal(snapshot.toString('hex'),'4154544f53543031013782cc560597ec4601000100000000000000002b35b70ef92e387a');
    return {module,sha:hash(result.module_source),descriptor:hash(descriptor)};
}
function guest(n,op,d) {
    for(const x of [n,op,d]) assert.ok(Number.isSafeInteger(x)&&x>=0&&x<=1000000000);
    const inst=backend==='wasm'?new WebAssembly.Instance(compiled.mod,{}):null;
    const entry=inst?inst.exports.atto_durable_transition_v1:jsCompiled.module.atto_durable_transition_v1;
    assert.equal(typeof entry,'function'); assert.equal(entry.length,3);
    const value=entry(BigInt(n),BigInt(op),BigInt(d));
    assert.equal(typeof value,'bigint');
    if(inst) assert.equal(inst.exports.panic_code.value,0);
    assert.ok(value>=-1n&&value<=1000000000n);
    executed++;
    receipts.push({source_sha256:hash(text),backend,artifact_sha256:backend==='wasm'?compiled.sha:jsCompiled.sha,...(backend==='wasm'?{wasm_sha256:compiled.sha}:{}),input:[n,op,d],result:String(value)});
    return value;
}
async function turn(mode,tenant,id,epoch,request,op,delta,expected,expectedExit=0,guestCount=1) {
    const before=executed;
    const child=spawn(driver,[mode,db,tenant,id,String(epoch),request,String(op),String(delta),hybrid?'fixture-v2':'fixture-v1'],
      {stdio:['pipe','pipe','pipe']});
    let errors='', failure, requests=0;
    const lines=[];
    const timer=setTimeout(()=>{failure=new Error('driver timeout');child.kill('SIGKILL');},45000);
    child.stderr.on('data',b=>{errors+=b.toString();if(errors.length>8192){failure=new Error('oversized stderr');child.kill('SIGKILL');}});
    child.stdin.on('error',e=>{failure??=e;child.kill('SIGKILL');});
    const reader=createInterface({input:child.stdout,crlfDelay:Infinity});
    reader.on('line',line=>{
        try {
            assert.ok(line.length<=128);
            if(line.startsWith('GUEST ')) {
                assert.equal(++requests,1,'multiple speculative guest executions');
                const m=/^GUEST (0|[1-9][0-9]{0,9}) (1|2) (0|[1-9][0-9]{0,9})$/.exec(line);
                assert.ok(m,'invalid guest protocol'); assert.equal(Number(m[2]),op); assert.equal(Number(m[3]),delta);
                child.stdin.end(String(guest(Number(m[1]),Number(m[2]),Number(m[3])))+'\n');
            } else { lines.push(line); assert.ok(lines.length<=1,'multiple terminal replies'); }
        } catch(e){failure=e;child.kill('SIGKILL');}
    });
    try {
        const result=await new Promise((res,rej)=>{
            child.once('error',rej);
            child.once('close',(code,signal)=>res({code,signal}));
        });
        if(failure) throw failure;
        if(expectedExit==='SIGKILL') { assert.equal(result.signal,'SIGKILL'); assert.equal(result.code,null); }
        else { assert.equal(result.signal,null); assert.equal(result.code,expectedExit,errors); }
        assert.deepEqual(lines,expected); assert.equal(executed-before,guestCount,'unexpected guest count');
        if(expectedExit===1) assert.match(errors, mode==='rollback' ? /injected failure before commit/ : epoch===1 ? /stale writer epoch/ : /operation id reused/, 'expected rejection diagnostic');
    } finally {clearTimeout(timer);reader.close();child.stdin.destroy();}
}
try {
    compiled=compileOnce();
    if(hybrid) jsCompiled=await compileJS();
    await turn('activate','a','same',1,'-',1,1,['ACTIVATED'],0,0);
    await turn('activate','b','same',1,'-',1,1,['ACTIVATED'],0,0);
    await turn('turn','a','same',1,'first',1,1,['COMMITTED 1 1 0']);
    if(hybrid) backend='js';
    await turn('rollback','a','same',1,'retry',1,2,[],1);
    await turn('turn','a','same',1,'retry',1,2,['COMMITTED 2 3 0']);
    await turn('lose','a','same',1,'max',1,999999997,[],'SIGKILL');
    if(hybrid) backend='wasm';
    await turn('activate','a','same',2,'-',1,1,['ACTIVATED'],0,0);
    await turn('turn','a','same',1,'max',1,999999997,[],1,0);
    await turn('turn','a','same',2,'max',1,999999997,['COMMITTED 3 1000000000 1'],0,0);
    await turn('turn','a','same',2,'max',1,1,[],1,0);
    await turn('turn','a','same',2,'overflow',1,1,['REJECTED']);
    await turn('turn','a','same',2,'read',2,0,['COMMITTED 4 1000000000 0']);
    if(hybrid) backend='js';
    await turn('turn','b','same',1,'max',1,1,['COMMITTED 1 1 0']);
    assert.equal(executed,7);
    if(hybrid) {
        // A single persisted logical identity changes compiler artifacts
        // between turns, including a real killed writer and new epoch.
        await turn('activate','a','hybrid',1,'-',1,1,['ACTIVATED'],0,0);
        for(let i=1;i<=99;i++){
            backend=i%2?'wasm':'js';
            await turn('turn','a','hybrid',1,'step'+i,1,1,['COMMITTED '+i+' '+i+' 0']);
        }
        backend='js';
        await turn('lose','a','hybrid',1,'step100',1,1,[],'SIGKILL');
        backend='wasm';
        await turn('activate','a','hybrid',2,'-',1,1,['ACTIVATED'],0,0);
        await turn('turn','a','hybrid',1,'step100',1,1,[],1,0);
        await turn('turn','a','hybrid',2,'step100',1,1,['COMMITTED 100 100 1'],0,0);
        await turn('turn','a','hybrid',2,'step101',1,1,['COMMITTED 101 101 0']);
        backend='js';
        await turn('turn','a','hybrid',2,'read',2,0,['COMMITTED 102 101 0']);
        assert.equal(executed,109);
        console.log('COMPILED_WASM_JS_SHARED_SQLITE_REVIVAL_100_TO_101_PASS');
    }
    console.log(JSON.stringify({kind:hybrid?'COMPILED_WASM_JS_SHARED_SQLITE_PROCESS_E2E':actorProfile?'COMPILED_DURABLE_ACTOR_SQLITE_PROCESS_E2E':'COMPILED_SCALAR_GUEST_SQLITE_PROCESS_E2E',compiler_binary_sha256:hash(readFileSync(compiler)),
      driver_binary_sha256:hash(readFileSync(driver)),compiler_source_head_claim:process.env.ATTO_COMPILER_SOURCE_SHA??null,
      fixture_sha256:hash(source),guest_executions:executed,compiler_invocations:hybrid?2:1,process_turns:hybrid?119:13,js_artifact_sha256:jsCompiled?.sha??null,descriptor_sha256:jsCompiled?.descriptor??null,receipts},null,2));
} finally {rmSync(dir,{recursive:true,force:true});}
