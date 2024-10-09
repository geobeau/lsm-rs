use core::str;
use std::{borrow::Cow, collections::HashMap};

use monoio::{io::BufReader, net::TcpListener};

use crate::{api, record, redis::{command::{ClientCmd, Command, RESPHandler}, resp::{redis_value_to_bytes, HashableValue, NonHashableValue, Value}}, storageproxy::StorageProxy};


// Serve the Redis serialization protocol (RESP)
pub struct RESPServer {
    pub host_port: String,
    pub storage_proxy: StorageProxy,
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
                        Err(err) => {
                            match err.kind() {
                                std::io::ErrorKind::ConnectionReset => break,
                                _ => {
                                    println!("Error on conn: {}", err);
                                    break
                                    },
                            }
                        },
                    };

                    let tmp_record: record::Record;
                    let resp = match redis_command {
                        Command::Hello(hello_cmd) => {
                            if hello_cmd.version != '3' {
                                Value::HashableValue(HashableValue::Error(Cow::from("NOPROTO"), Cow::from("sorry, this protocol version is not supported.")));
                            }
                            Value::NonHashableValue(NonHashableValue::Map(HashMap::from([
                                (HashableValue::String(Cow::from("server")), Value::HashableValue(HashableValue::String(Cow::from("redis")))),
                                (HashableValue::String(Cow::from("version")), Value::HashableValue(HashableValue::String(Cow::from("0")))),
                                (HashableValue::String(Cow::from("proto")), Value::HashableValue(HashableValue::Integer(3))),
                                (HashableValue::String(Cow::from("id")), Value::HashableValue(HashableValue::Integer(0))),
                                (HashableValue::String(Cow::from("mode")), Value::HashableValue(HashableValue::String(Cow::from("standalone")))),
                                (HashableValue::String(Cow::from("modules")), Value::Null),
                                ])
                            ))

                        },
                        Command::Client(client_cmd) => match client_cmd {
                            ClientCmd::SetInfo(set_info_cmd) => Value::HashableValue(HashableValue::String(Cow::from("OK"))),
                        },
                        Command::Set(set_cmd) => {
                            // TODO: should return result
                            let _ = storage_proxy.dispatch(set_cmd.to_api_command()).await;
                            Value::HashableValue(HashableValue::String(Cow::from("OK")))
                        },
                        Command::Get(get_cmd) => {
                            if let api::Response::Get(resp) = storage_proxy.dispatch(get_cmd.to_api_command()).await {
                                match resp.record {
                                    Some(r) => {
                                        tmp_record = r;
                                        Value::HashableValue(HashableValue::Blob(&tmp_record.value))},
                                    None => Value::Null,
                                }
                            } else {
                                panic!("Unexpected response")
                            }
                        },
                    };

                    let mut resp_bytes = vec![];
                    redis_value_to_bytes(&resp, &mut resp_bytes);
                    // println!("Answering: {:?}", str::from_utf8(&resp_bytes).unwrap());
                    handler.write_resp(resp_bytes).await;
                }
            });
        }
    }
}