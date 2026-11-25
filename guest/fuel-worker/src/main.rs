//! Trusted Linux scalar Wasm interpreter worker. No WASI or imports.
//! Engine-specific fuel, not a portable AttoLang cost schedule.
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read};
use wasmi::{core::TrapCode, Config, Engine, Linker, Module, Store, StoreLimitsBuilder, Value};

const FUEL: u64 = 50_000;
const CAP: usize = 1024 * 1024;
const INVALID: i32 = 74;
const TRAP: i32 = 73;
const FUEL_EXHAUSTED: i32 = 75;

fn limit(resource: libc::__rlimit_resource_t, value: libc::rlim_t) -> Result<(), i32> {
    let lim = libc::rlimit { rlim_cur: value, rlim_max: value };
    // SAFETY: valid pointer to a local rlimit; only this child is affected.
    if unsafe { libc::setrlimit(resource, &lim) } != 0 {
        return Err(INVALID);
    }
    Ok(())
}

fn contain(parent: libc::pid_t) -> Result<(), i32> {
    // SAFETY: process-local Linux prctl and getppid; no pointer arguments.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0
        || unsafe { libc::getppid() } != parent
    {
        return Err(INVALID);
    }
    limit(libc::RLIMIT_CORE, 0)?;
    limit(libc::RLIMIT_AS, 256 * 1024 * 1024)?;
    limit(libc::RLIMIT_CPU, 2)?;
    limit(libc::RLIMIT_NOFILE, 16)?;
    Ok(())
}

fn scalar(value: &str) -> Result<i64, i32> {
    let n = value.parse::<i64>().map_err(|_| INVALID)?;
    if !(0..=1_000_000_000).contains(&n) || n.to_string() != value {
        return Err(INVALID);
    }
    Ok(n)
}

fn run() -> Result<i64, i32> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [artifact, digest, n, op, delta, parent] = args.as_slice() else {
        return Err(INVALID);
    };
    contain(parent.parse().map_err(|_| INVALID)?)?;
    let (n, op, delta) = (scalar(n)?, scalar(op)?, scalar(delta)?);
    if !matches!(op, 1 | 2) || digest.len() != 64 {
        return Err(INVALID);
    }
    let mut bytes = Vec::new();
    File::open(artifact).map_err(|_| INVALID)?
        .take((CAP + 1) as u64).read_to_end(&mut bytes).map_err(|_| INVALID)?;
    if bytes.is_empty() || bytes.len() > CAP || format!("{:x}", Sha256::digest(&bytes)) != *digest {
        return Err(INVALID);
    }
    let mut config = Config::default();
    config.consume_fuel(true);
    let engine = Engine::new(&config);
    let module = Module::new(&engine, &mut &bytes[..]).map_err(|_| INVALID)?;
    if module.imports().next().is_some() {
        return Err(INVALID);
    }
    let mut exports: Vec<_> = module.exports().map(|e| e.name().to_owned()).collect();
    exports.sort();
    if exports != ["atto_durable_transition_v1", "main", "panic_code"] {
        return Err(INVALID);
    }
    // The scalar ABI admits no linear memory or table, even if not exported.
    let limits = StoreLimitsBuilder::new().memories(0).tables(0).instances(1).build();
    let mut store = Store::new(&engine, limits);
    store.limiter(|limits| limits);
    store.add_fuel(FUEL).map_err(|_| INVALID)?;
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &module).map_err(|_| INVALID)?
        .ensure_no_start(&mut store).map_err(|_| INVALID)?;
    let panic = instance.get_global(&store, "panic_code").ok_or(INVALID)?;
    if !matches!(panic.get(&store), Value::I32(0)) {
        return Err(INVALID);
    }
    let entry = instance.get_typed_func::<(i64, i64, i64), i64>(&store, "atto_durable_transition_v1")
        .map_err(|_| INVALID)?;
    let result = entry.call(&mut store, (n, op, delta)).map_err(|trap| {
        if matches!(trap.trap_code(), Some(TrapCode::OutOfFuel)) {
            FUEL_EXHAUSTED
        } else {
            TRAP
        }
    })?;
    if !matches!(panic.get(&store), Value::I32(0)) {
        return Err(TRAP);
    }
    if !(-1..=1_000_000_000).contains(&result) {
        return Err(INVALID);
    }
    Ok(result)
}

fn main() {
    match run() {
        Ok(value) => println!("{value}"),
        Err(code) => std::process::exit(code),
    }
}
