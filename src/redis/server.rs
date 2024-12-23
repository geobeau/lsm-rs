use std::{borrow::Cow, collections::HashMap, rc::Rc, vec};

use monoio::{io::BufReader, net::TcpListener};

use crate::{
    api,
    redis::{
        command::{ClientCmd, Command, RESPHandler},
        resp::{HashableValue, NonHashableValue, Value},
    },
    storageproxy::StorageProxy,
    topology::Topology,
};

use super::serde::ToResp;

// Serve the Redis serialization protocol (RESP)
pub struct RESPServer {
    pub host_port: String,
    pub storage_proxy: Rc<StorageProxy>,
}

// Return a redis compatible topology
fn cluster_shards_response(topology: &Topology) -> Value {
    let shards = topology
        .reactor_allocations
        .iter()
        .flat_map(|(reactor, ranges)| {
            let resp_ranges: Vec<Value> = ranges
                .iter()
                .map(|range| {
                    Value::NonHashableValue(NonHashableValue::Array(vec![
                        // Range start
                        Value::HashableValue(HashableValue::Integer(range.start as i64)),
                        // Range end
                        Value::HashableValue(HashableValue::Integer(range.end as i64)),
                        // Primary node
                        Value::NonHashableValue(NonHashableValue::Array(vec![
                            // TODO fix this :'(
                            // Cannot borrow reactor data as it create temporaty value
                            // Value::HashableValue(HashableValue::Blob(reactor.ip.to_string().clone().as_bytes())),
                            Value::HashableValue(HashableValue::Blob("127.0.0.1".as_bytes())),
                            Value::HashableValue(HashableValue::Integer(reactor.port as i64)),
                            Value::HashableValue(HashableValue::String(Cow::from(format!("{}", range.start)))),
                            Value::NonHashableValue(NonHashableValue::Array(vec![
                                Value::HashableValue(HashableValue::String(Cow::from("hostname"))),
                                Value::HashableValue(HashableValue::String(Cow::from(reactor.ip.to_string()))),
                            ])),
                        ])),
                    ]))
                })
                .collect();
            resp_ranges
        })
        .collect();

    return Value::NonHashableValue(NonHashableValue::Array(shards));
}

impl RESPServer {
    pub async fn listen(self) -> ! {
        let listener = TcpListener::bind(self.host_port.clone()).unwrap();

        println!("Listening on {}", listener.local_addr().unwrap());
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let storage_proxy = self.storage_proxy.clone();
            let reader = BufReader::new(stream);
            monoio::spawn(async move {
                let mut handler = RESPHandler { stream: reader };
                loop {
                    let redis_command = match handler.decode_command().await {
                        Ok(c) => c,
                        Err(err) => match err.kind() {
                            std::io::ErrorKind::ConnectionReset => break,
                            _ => {
                                println!("Error on conn: {}", err);
                                break;
                            }
                        },
                    };

                    // let tmp_record: record::Record;
                    let resp_bytes: Vec<u8> = match redis_command {
                        Command::Hello(hello_cmd) => {
                            if hello_cmd.version != '3' {
                                Value::HashableValue(HashableValue::Error(
                                    Cow::from("NOPROTO"),
                                    Cow::from("sorry, this protocol version is not supported."),
                                ))
                                .to_bytes()
                            } else {
                                Value::NonHashableValue(NonHashableValue::Map(HashMap::from([
                                    (
                                        HashableValue::String(Cow::from("server")),
                                        Value::HashableValue(HashableValue::String(Cow::from("redis"))),
                                    ),
                                    (
                                        HashableValue::String(Cow::from("version")),
                                        Value::HashableValue(HashableValue::String(Cow::from("0"))),
                                    ),
                                    (HashableValue::String(Cow::from("proto")), Value::HashableValue(HashableValue::Integer(3))),
                                    (HashableValue::String(Cow::from("id")), Value::HashableValue(HashableValue::Integer(0))),
                                    (
                                        HashableValue::String(Cow::from("mode")),
                                        Value::HashableValue(HashableValue::String(Cow::from("cluster"))),
                                    ),
                                    (HashableValue::String(Cow::from("modules")), Value::Null),
                                ])))
                                .to_bytes()
                            }
                        }
                        Command::Client(client_cmd) => match client_cmd {
                            ClientCmd::SetInfo(_) => Value::HashableValue(HashableValue::String(Cow::from("OK"))).to_bytes(),
                        },
                        Command::Set(set_cmd) => {
                            // TODO: should return result
                            let _ = storage_proxy.dispatch(set_cmd.to_api_command()).await;
                            Value::HashableValue(HashableValue::String(Cow::from("OK"))).to_bytes()
                        }
                        Command::Get(get_cmd) => {
                            if let api::Response::Get(resp) = storage_proxy.dispatch(get_cmd.to_api_command()).await {
                                match resp.record {
                                    Some(r) => Value::HashableValue(HashableValue::Blob(&r.value)).to_bytes(),
                                    None => Value::Null.to_bytes(),
                                }
                            } else {
                                panic!("Unexpected response")
                            }
                        }
                        Command::Cluster(cluster_cmd) => match cluster_cmd {
                            crate::redis::command::ClusterCmd::Join(join_cmd) => {
                                if let api::Response::ClusterTopology(resp) = storage_proxy.dispatch(join_cmd.to_api_command()).await {
                                    resp.topology.to_resp().to_bytes()
                                } else {
                                    panic!("Unexpected response")
                                }
                            }
                            crate::redis::command::ClusterCmd::Info() => Value::NonHashableValue(NonHashableValue::Map(HashMap::from([
                                (
                                    HashableValue::String(Cow::from("cluster_state")),
                                    Value::HashableValue(HashableValue::String(Cow::from("ok"))),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_shards_assigned")),
                                    Value::HashableValue(HashableValue::Integer(16384)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_shards_ok")),
                                    Value::HashableValue(HashableValue::Integer(16384)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_shards_pfail")),
                                    Value::HashableValue(HashableValue::Integer(0)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_shards_fail")),
                                    Value::HashableValue(HashableValue::Integer(0)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_known_nodes")),
                                    Value::HashableValue(HashableValue::Integer(1)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_size")),
                                    Value::HashableValue(HashableValue::Integer(1)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_current_epoch")),
                                    Value::HashableValue(HashableValue::Integer(1)),
                                ),
                                (
                                    HashableValue::String(Cow::from("cluster_my_epoch")),
                                    Value::HashableValue(HashableValue::Integer(1)),
                                ),
                            ])))
                            .to_bytes(),

                            crate::redis::command::ClusterCmd::Slots() => {
                                let topology = storage_proxy.get_topology().unwrap();
                                cluster_shards_response(&topology).to_bytes()
                            }
                        },
                        Command::Command() => Value::NonHashableValue(NonHashableValue::Array(vec![
                            // TODO: get that through reflection
                            Value::NonHashableValue(NonHashableValue::Array(vec![
                                // Name
                                Value::HashableValue(HashableValue::String(Cow::from("SET"))),
                                // Arity is the number of arguments a command expects
                                Value::HashableValue(HashableValue::Integer(3)),
                                // Flags
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // First key
                                Value::HashableValue(HashableValue::Integer(1)),
                                // Last Key
                                Value::HashableValue(HashableValue::Integer(1)),
                                // Step
                                Value::HashableValue(HashableValue::Integer(1)),
                                // ACLs categories
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Tips
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Key specs
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Sub commands
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                            ])),
                            Value::NonHashableValue(NonHashableValue::Array(vec![
                                // Name
                                Value::HashableValue(HashableValue::String(Cow::from("GET"))),
                                // Arity is the number of arguments a command expects
                                Value::HashableValue(HashableValue::Integer(2)),
                                // Flags
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // First key
                                Value::HashableValue(HashableValue::Integer(1)),
                                // Last Key
                                Value::HashableValue(HashableValue::Integer(1)),
                                // Step
                                Value::HashableValue(HashableValue::Integer(1)),
                                // ACLs categories
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Tips
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Key specs
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                                // Sub commands
                                Value::NonHashableValue(NonHashableValue::Array(vec![])),
                            ])),
                        ]))
                        .to_bytes(),
                    };

                    // println!("Answering: {:?}", str::from_utf8(&resp_bytes).unwrap());
                    handler.write_resp(resp_bytes).await;
                }
            });
        }
    }
}
