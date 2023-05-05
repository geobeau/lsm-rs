use crate::record::Record;

pub enum Command {
    Get(Get),
    Delete(Delete),
    Set(Set),
}

pub struct Get {
    key: String,
}

pub struct Delete {
    key: String,
}

pub struct Set {
    record: Record
}
