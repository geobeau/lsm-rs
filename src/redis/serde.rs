use std::{borrow::Cow, collections::HashMap};

use crate::{
    redis::resp::NonHashableValue,
    topology::{ReactorMetadata, ShardRange, Topology},
};

use super::resp::{HashableValue, Value};

pub trait ToResp {
    fn to_resp(&self) -> Value;
}

pub trait FromResp {
    fn from_resp(value: &Value) -> Self;
}

impl ToResp for ReactorMetadata {
    fn to_resp(&self) -> Value {
        let mut map = HashMap::with_capacity(4);
        map.insert(
            HashableValue::String(Cow::from("node_id")),
            Value::HashableValue(HashableValue::String(Cow::from(format!("{}", self.node_id)))),
        );
        map.insert(
            HashableValue::String(Cow::from("id")),
            Value::HashableValue(HashableValue::String(Cow::from(format!("{}", self.id)))),
        );
        map.insert(
            HashableValue::String(Cow::from("ip")),
            Value::HashableValue(HashableValue::String(Cow::from(format!("{}", self.ip)))),
        );
        map.insert(
            HashableValue::String(Cow::from("port")),
            Value::HashableValue(HashableValue::String(Cow::from(format!("{}", self.port)))),
        );
        Value::NonHashableValue(NonHashableValue::Map(map))
    }
}

impl FromResp for ReactorMetadata {
    fn from_resp(value: &Value) -> Self {
        let raw_reactor = match value {
            Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                NonHashableValue::Map(vec) => vec,
                _ => todo!(),
            },
            _ => todo!(),
        };

        let node_id = raw_reactor.get(&HashableValue::String(Cow::from("node_id"))).unwrap();
        let id = raw_reactor.get(&HashableValue::String(Cow::from("id"))).unwrap();
        let ip = raw_reactor.get(&HashableValue::String(Cow::from("ip"))).unwrap();
        let port = raw_reactor.get(&HashableValue::String(Cow::from("port"))).unwrap();

        ReactorMetadata {
            node_id: node_id.try_as_str().unwrap().parse().unwrap(),
            id: id.try_as_str().unwrap().parse().unwrap(),
            ip: ip.try_as_str().unwrap().parse().unwrap(),
            port: port.try_as_str().unwrap().parse().unwrap(),
        }
    }
}

impl ToResp for ShardRange {
    fn to_resp(&self) -> Value {
        return Value::NonHashableValue(NonHashableValue::Array(vec![
            Value::HashableValue(HashableValue::Integer(self.start as i64)),
            Value::HashableValue(HashableValue::Integer(self.end as i64)),
        ]));
    }
}

impl FromResp for ShardRange {
    fn from_resp(value: &Value) -> Self {
        let raw_shard = match value {
            Value::HashableValue(_) => todo!(),
            Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                NonHashableValue::Array(vec) => vec,
                _ => todo!(),
            },
            Value::Null => todo!(),
        };

        let start = match &raw_shard[0] {
            Value::HashableValue(hashable_value) => match hashable_value {
                HashableValue::Integer(i) => *i as u16,
                _ => todo!(),
            },
            _ => todo!(),
        };
        let end = match &raw_shard[1] {
            Value::HashableValue(hashable_value) => match hashable_value {
                HashableValue::Integer(i) => *i as u16,
                _ => todo!(),
            },
            _ => todo!(),
        };

        ShardRange { start, end }
    }
}

impl ToResp for Topology {
    fn to_resp(&self) -> Value {
        let shards = self
            .reactor_allocations
            .iter()
            .map(|(reactor, ranges)| {
                let resp_ranges: Vec<Value<'_>> = ranges.iter().map(|shard_range| shard_range.to_resp()).collect();
                Value::NonHashableValue(NonHashableValue::Array(vec![
                    reactor.to_resp(),
                    Value::NonHashableValue(NonHashableValue::Array(resp_ranges)),
                ]))
            })
            .collect();

        return Value::NonHashableValue(NonHashableValue::Array(vec![
            Value::HashableValue(HashableValue::Integer(self.shards_count as i64)),
            Value::NonHashableValue(NonHashableValue::Array(shards)),
        ]));
    }
}

impl FromResp for Topology {
    fn from_resp(value: &Value) -> Self {
        let args = match value {
            Value::HashableValue(_) => todo!(),
            Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                NonHashableValue::Array(vec) => vec,
                _ => todo!(),
            },
            Value::Null => todo!(),
        };

        let shards_count = match &args[0] {
            Value::HashableValue(hashable_value) => match hashable_value {
                HashableValue::Integer(i) => *i as u16,
                _ => todo!(),
            },
            _ => todo!(),
        };

        let raw_shards = match &args[1] {
            Value::HashableValue(_) => todo!(),
            Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                NonHashableValue::Array(vec) => vec,
                _ => todo!(),
            },
            Value::Null => todo!(),
        };

        let mut reactor_allocations = HashMap::new();
        raw_shards.iter().for_each(|raw_shard| {
            let shard_tuple = match raw_shard {
                Value::HashableValue(_) => todo!(),
                Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                    NonHashableValue::Array(vec) => vec,
                    _ => todo!(),
                },
                Value::Null => todo!(),
            };

            let reactor_metadata = ReactorMetadata::from_resp(&shard_tuple[0]);

            let raw_ranges = match &shard_tuple[1] {
                Value::HashableValue(_) => todo!(),
                Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                    NonHashableValue::Array(vec) => vec,
                    _ => todo!(),
                },
                Value::Null => todo!(),
            };
            let ranges = raw_ranges.iter().map(|raw_range| ShardRange::from_resp(raw_range)).collect();
            reactor_allocations.insert(reactor_metadata, ranges);
        });
        Topology {
            shards_count,
            reactor_allocations,
        }
    }
}
