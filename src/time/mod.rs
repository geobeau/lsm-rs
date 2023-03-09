use std::{
    cell::Cell,
    cmp::max,
    time::{SystemTime, UNIX_EPOCH},
};

thread_local! {
    pub static OLD_NOW: std::cell::Cell<u64> = Cell::new(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64);
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

/// Sync make sure external timestamp are correctly taken into account
/// This is useful in case of a restart where the time is now older than before the restart
/// When reading from disk, timestamp could be seen in the future so it's important to sync them
pub fn sync(ts: u64) {
    OLD_NOW.with(|x| {
        let mut val = x.get();
        val = max(ts + 1, val);
        x.set(val);
    })
}
