// Real compiled scalar guest + canonical SQLite host store across OS processes.
// Test-only driver, sequential trusted scheduling, no remote ingress.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';

assert.ok(process.env.ATTOC && process.env.COUNTER_DRIVER, 'real compiler and host driver required');
const compiler=resolve(process.env.ATTOC), driver=resolve(process.env.COUNTER_DRIVER);
const source=readFileSync(new URL('../guest/durable_counter.atto',import.meta.url),'utf8');
const dir=mkdtempSync(join(tmpdir(),'atto-counter-e2e-'));
const db=join(dir,'counter.db');
const hash=b=>createHash('sha256').update(b).digest('hex');
let executed=0;
const receipts=[];
function guest(n,op,d) {
    for(const x of [n,op,d]) assert.ok(Number.isSafeInteger(x)&&x>=0&&x<=1000000000);
    const text=source+'\npub fnc main() -> int { return durable_counter_next('+n+', '+op+', '+d+'); }\n';
    const input=join(dir,'turn.atto'), output=join(dir,'turn.wasm');
    writeFileSync(input,text); rmSync(output,{force:true});
    const r=spawnSync(compiler,[input,'-o',output],{cwd:dir,encoding:'utf8',timeout:30000,maxBuffer:1<<20});
    assert.equal(r.error,undefined); assert.equal(r.status,0,r.stderr);
    assert.ok(existsSync(output),'real compiler did not emit artifact');
    const bytes=readFileSync(output);
    assert.ok(WebAssembly.validate(bytes));
    const mod=new WebAssembly.Module(bytes);
    assert.deepEqual(WebAssembly.Module.imports(mod),[]);
    const inst=new WebAssembly.Instance(mod,{});
    const value=inst.exports.main();
    assert.equal(typeof value,'bigint');
    assert.equal(inst.exports.panic_code.value,0);
    assert.ok(value>=-1n&&value<=1000000000n);
    executed++;
    receipts.push({source_sha256:hash(text),wasm_sha256:hash(bytes),input:[n,op,d],result:String(value)});
    return value;
}
async function turn(mode,tenant,id,epoch,request,op,delta,expected,expectedExit=0,guestCount=1) {
    const before=executed;
    const child=spawn(driver,[mode,db,tenant,id,String(epoch),request,String(op),String(delta),'fixture-v1'],
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
        assert.equal(result.signal,null); assert.equal(result.code,expectedExit,errors);
        assert.deepEqual(lines,expected); assert.equal(executed-before,guestCount,'unexpected guest count');
        if(expectedExit===1) assert.ok(errors.trim(),'expected rejection diagnostic');
    } finally {clearTimeout(timer);reader.close();child.stdin.destroy();}
}
try {
    await turn('activate','a','same',1,'-',1,1,['ACTIVATED'],0,0);
    await turn('activate','b','same',1,'-',1,1,['ACTIVATED'],0,0);
    await turn('turn','a','same',1,'first',1,1,['COMMITTED 1 1 0']);
    await turn('rollback','a','same',1,'retry',1,2,[],1);
    await turn('turn','a','same',1,'retry',1,2,['COMMITTED 2 3 0']);
    await turn('lose','a','same',1,'max',1,999999997,[],42);
    await turn('activate','a','same',2,'-',1,1,['ACTIVATED'],0,0);
    await turn('turn','a','same',1,'max',1,999999997,[],1,0);
    await turn('turn','a','same',2,'max',1,999999997,['COMMITTED 3 1000000000 1'],0,0);
    await turn('turn','a','same',2,'max',1,1,[],1,0);
    await turn('turn','a','same',2,'overflow',1,1,['REJECTED']);
    await turn('turn','a','same',2,'read',2,0,['COMMITTED 4 1000000000 0']);
    await turn('turn','b','same',1,'max',1,1,['COMMITTED 1 1 0']);
    assert.equal(executed,7);
    console.log(JSON.stringify({kind:'COMPILED_SCALAR_GUEST_SQLITE_PROCESS_E2E',compiler_binary_sha256:hash(readFileSync(compiler)),
      driver_binary_sha256:hash(readFileSync(driver)),compiler_source_head_claim:process.env.ATTO_COMPILER_SOURCE_SHA??null,
      fixture_sha256:hash(source),guest_executions:executed,process_turns:13,receipts},null,2));
} finally {rmSync(dir,{recursive:true,force:true});}
