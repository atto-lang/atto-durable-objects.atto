// Trusted fixed child entry: no arbitrary JS or host imports are exposed.
// The parent owns the wall deadline and waits for physical exit.
import { createHash } from 'node:crypto';
import { openSync, closeSync, readSync } from 'node:fs';
const [artifact,digest,n,op,d,...extra]=process.argv.slice(2);
function requireValid(ok) { if(!ok) throw new Error('invalid worker admission'); }
function scalar(x) {
    requireValid(typeof x==='string' && /^(0|[1-9][0-9]{0,9})$/.test(x));
    const v=BigInt(x); requireValid(v<=1000000000n); return v;
}
try {
    requireValid(extra.length===0 && typeof digest==='string' && /^[a-f0-9]{64}$/.test(digest));
    const args=[scalar(n),scalar(op),scalar(d)];
    requireValid(args[1]===1n || args[1]===2n);
    const buffer=Buffer.alloc(1024*1024+1);
    const fd=openSync(artifact,'r');let size=0;
    try {
        while(size<buffer.length) {
            const count=readSync(fd,buffer,size,buffer.length-size,null);
            if(count===0)break;
            size+=count;
        }
    } finally {closeSync(fd);}
    requireValid(size>0 && size<buffer.length);
    const bytes=buffer.subarray(0,size);
    requireValid(createHash('sha256').update(bytes).digest('hex')===digest);
    const module=new WebAssembly.Module(bytes);
    requireValid(WebAssembly.Module.imports(module).length===0);
    requireValid(WebAssembly.Module.exports(module).map(x=>x.name).sort().join(',')===
        'atto_durable_transition_v1,main,panic_code');
    const {atto_durable_transition_v1:entry,panic_code:panic}=new WebAssembly.Instance(module,{}).exports;
    requireValid(typeof entry==='function' && entry.length===3 && panic instanceof WebAssembly.Global && panic.value===0);
    let value;
    try {value=entry(...args);}
    catch(e) {
        // Only an actual Wasm guest trap gets a classified status.
        if(e instanceof WebAssembly.RuntimeError) process.exit(73);
        throw e;
    }
    if(panic.value!==0) process.exit(73);
    requireValid(typeof value==='bigint' && value>=-1n && value<=1000000000n);
    process.stdout.write(String(value)+'\n');
} catch {
    process.exitCode=74;
}
