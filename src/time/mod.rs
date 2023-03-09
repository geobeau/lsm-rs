use std::{
    cell::Cell,
    cmp::max,
    time::{SystemTime, UNIX_EPOCH},
};

thread_local! {
    pub static OLD_NOW: std::cell::Cell<u64> = Cell::new(0);
}

/// Basic implementation for an hybrid/monotonic timestamp. Precision is ms.
/// In case 2 timestamps get requested in the same ms, second one is increased by 1
/// In case now is older the previously generated timestamp, return previous + 1
pub fn now() -> u64 {
    OLD_NOW.with(|x| {
        let mut val = x.get();
        val = max(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64, val + 1);
        x.set(val);
        x.get()
    })
}
