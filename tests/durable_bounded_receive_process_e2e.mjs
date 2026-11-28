// Test-only integration of a real restricted compiled receive and actual SQLite
// broker. The harness is trusted and supplies the artifact digest; not production
// guest authentication, scheduler integration or durable mailbox admission.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
assert.ok(process.env.ATTO_JS_RECEIVE_COMPILER&&process.env.ATTO_BOUNDED_DRIVER);
const {compile_durable_bounded_receive_v2:compile}=await import(pathToFileURL(resolve(process.env.ATTO_JS_RECEIVE_COMPILER)));
const source='define durable actor Ledger as state count: int = 0; state name: string = ""; receive(ActorMail<string> mail) -> void { self.count = self.count + 1; self.name = mail.value; } end';
const c=compile(source);assert.equal(c.guest_abi,'atto.js.durable.bounded-receive.v2.draft');
const digest=createHash('sha256').update(c.module_source).digest('hex');
const guest=await import('data:text/javascript;base64,'+Buffer.from(c.module_source).toString('base64'));
const dir=mkdtempSync(join(tmpdir(),'atto-bounded-receive-')),db=join(dir,'actor.db');
const hex=b=>Buffer.from(b).toString('hex');
const unhex=s=>{assert.match(s,/^(?:[0-9a-f]{2})*$/);return Uint8Array.from(Buffer.from(s,'hex'));};
let dispatched=0;
async function turn(mode,epoch,id,mail,expected,{corrupt=false}={}){
 const args=[mode,db,String(epoch),id,hex(new TextEncoder().encode(mail)),digest];
 return new Promise((done,fail)=>{
  const child=spawn(resolve(process.env.ATTO_BOUNDED_DRIVER),args,{stdio:['pipe','pipe','pipe']});
  let stdout='',stderr='',line='',calls=0,failed=null;
  const abort=e=>{failed=e;child.kill('SIGKILL');};
  const timer=setTimeout(()=>abort(new Error('bounded driver timeout')),20000);
  child.stdin.on('error',e=>{if(e.code!=='EPIPE')abort(e);});
  child.on('error',e=>{clearTimeout(timer);fail(e);});
  child.stderr.on('data',b=>{stderr+=b.toString();if(stderr.length>32768)abort(new Error('stderr overflow'));});
  child.stdout.on('data',b=>{
   try{
    stdout+=b.toString();line+=b.toString();if(stdout.length>262400)throw new Error('stdout overflow');
    let n;while((n=line.indexOf('\n'))>=0){const next=line.slice(0,n);line=line.slice(n+1);
     if(next.startsWith('GUEST ')){
      assert.equal(++calls,1);dispatched++;
      const parts=next.split(' ');assert.equal(parts.length,3);
      const message=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(unhex(parts[2]));assert.equal(message,mail);
      const result=guest.atto_receive_v2(unhex(parts[1]),message);
      if(corrupt)result[0]^=1;
      child.stdin.end(hex(result)+'\n');
     }
    }
   }catch(e){abort(e);}
  });
  child.on('close',(code,signal)=>{
   clearTimeout(timer);
   try{
    if(failed)throw failed;
    assert.equal(calls,expected.calls);
    const lines=stdout.trimEnd().split('\n'),reply=lines.filter(x=>x.startsWith('COMMITTED '));
    if(expected.kill){assert.equal(signal,'SIGKILL');assert.equal(reply.length,0);}
    else if(expected.error){assert.equal(signal,null);assert.equal(code,1,stderr);assert.equal(reply.length,0);}
    else if(mode==='activate'){assert.equal(code,0,stderr);assert.equal(stdout.trim(),'ACTIVATED');}
    else{assert.equal(code,0,stderr);assert.equal(signal,null);assert.equal(reply.length,1);const [,rev,state,dup]=reply[0].split(' ');assert.equal(Number(rev),expected.rev);assert.equal(dup,expected.duplicate?'1':'0');assert.deepEqual(guest.atto_decode_state_v2(unhex(state)),[BigInt(expected.rev),expected.name]);}
    done();
   }catch(e){fail(e);}
  });
 });
}
try{
 const first='é😀\0first',second='second',third='third';
 await turn('activate',1,'init','',{calls:0});
 await turn('lose',1,'first',first,{calls:1,kill:true});
 await turn('activate',2,'init','',{calls:0});
 await turn('turn',1,'first',first,{calls:0,error:true});
 await turn('turn',2,'first',first,{calls:0,rev:1,name:first,duplicate:true});
 await turn('turn',2,'first','changed',{calls:0,error:true});
 await turn('rollback',2,'second',second,{calls:1,error:true});
 await turn('turn',2,'second',second,{calls:1,rev:2,name:second});
 await turn('turn',2,'bad','bad',{calls:1,error:true},{corrupt:true});
 await turn('turn',2,'third',third,{calls:1,rev:3,name:third});
 await turn('turn',2,'third',third,{calls:0,rev:3,name:third,duplicate:true});
 assert.equal(dispatched,5);
 console.log(JSON.stringify({status:'COMPILED_JS_BOUNDED_RECEIVE_SQLITE_PASS',commits:3,physical_kills:1,rollback:1,replays:2,guest_dispatches:5,artifact_sha256:digest,production_scheduler:false}));
}finally{rmSync(dir,{recursive:true,force:true});}
