use core::str;
use std::borrow::Cow;

use monoio::io::{AsyncBufRead, AsyncWriteRentExt, BufReader};

use crate::{
    api::{self, Join},
    record::{Key, Record},
    redis::resp::{parse, NonHashableValue},
    topology::ReactorMetadata,
};

use super::resp::{HashableValue, Value};

#[derive(Debug, Clone)]
pub enum Command {
    Hello(HelloCmd),
    Client(ClientCmd),
    Cluster(ClusterCmd),
    Command(),
    Set(SetCmd),
    Get(GetCmd),
}

#[derive(Debug, Clone)]
pub struct SetInfoCmd {
    pub lib_name: Option<String>,
    pub lib_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HelloCmd {
    pub version: char,
}

const CMD_HELLO: &str = "HELLO";
fn parse_hello_command(args: &[Value]) -> Command {
    let version = match &args[1] {
        Value::HashableValue(hashable_value) => match hashable_value {
            HashableValue::Blob(vec) => vec,
            _ => todo!(),
        },
        Value::NonHashableValue(_) => todo!(),
        Value::Null => todo!(),
    };
    Command::Hello(HelloCmd { version: version[0] as char })
}

#[derive(Debug, Clone)]
pub enum ClientCmd {
    SetInfo(SetInfoCmd),
}

const CMD_CLIENT: &str = "CLIENT";
fn parse_client_command(args: &[Value]) -> Command {
    let sub_command = args[1].try_as_str().unwrap();
    match sub_command {
        CMD_SETINFO => Command::Client(ClientCmd::SetInfo(parse_setinfo_cmd(args))),
        _ => todo!(),
    }
}

const CMD_SETINFO: &str = "SETINFO";
fn parse_setinfo_cmd(args: &[Value]) -> SetInfoCmd {
    let _ = args[2].try_as_str().unwrap();
    let value = args[3].try_as_str().unwrap();

    SetInfoCmd {
        lib_name: Some(String::from(value)),
        lib_type: None,
    }
}

#[derive(Debug, Clone)]
pub struct SetCmd {
    pub key: String,
    pub value: Vec<u8>,
}

impl SetCmd {
    pub fn to_api_command(&self) -> api::Command {
        api::Command::Data(api::DataCommand::Set(api::Set {
            record: Record::new(self.key.clone(), self.value.clone()),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct GetCmd {
    pub key: String,
}

impl GetCmd {
    pub fn to_api_command(&self) -> api::Command {
        api::Command::Data(api::DataCommand::Get(api::Get {
            key: Key::new(self.key.clone()),
        }))
    }
}

const CMD_SET: &str = "SET";
fn parse_set_command(args: &[Value]) -> Command {
    let key = args[1].try_as_str().unwrap();
    let value = args[2].try_as_str().unwrap();

    Command::Set(SetCmd {
        key: String::from(key),
        value: Vec::from(value),
    })
}

const CMD_GET: &str = "GET";
fn parse_get_command(args: &[Value]) -> Command {
    let key = args[1].try_as_str().unwrap();

    Command::Get(GetCmd { key: String::from(key) })
}

#[derive(Debug, Clone)]
pub enum ClusterCmd {
    Slots(),
    Info(),
    Join(JoinCmd),
}

const CMD_CLUSTER_SLOT: &str = "SLOTS";
const CMD_CLUSTER_INFO: &str = "INFO";

const CMD_CLUSTER_JOIN: &str = "JOIN";
#[derive(Debug, Clone)]
pub struct JoinCmd {
    reactors: Vec<ReactorMetadata>,
}

impl JoinCmd {
    pub fn to_api_command(&self) -> api::Command {
        api::Command::Cluster(api::ClusterCommand::Join(Join {
            reactors: self.reactors.clone(),
        }))
    }
}

fn parse_cluster_join_command(args: &[Value]) -> Command {
    let raw_reactors = match &args[2] {
        Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
            NonHashableValue::Array(vec) => vec,
            _ => todo!(),
        },
        _ => todo!(),
    };

    let reactors = raw_reactors
        .iter()
        .map(|value| {
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
        })
        .collect();

    Command::Cluster(ClusterCmd::Join(JoinCmd { reactors }))
}

const CMD_CLUSTER: &str = "CLUSTER";
fn parse_cluster_command(args: &[Value]) -> Command {
    let sub_command = args[1].try_as_str().unwrap();
    match sub_command {
        CMD_CLUSTER_SLOT => Command::Cluster(ClusterCmd::Slots()),
        CMD_CLUSTER_INFO => Command::Cluster(ClusterCmd::Info()),
        CMD_CLUSTER_JOIN => parse_cluster_join_command(args),
        _ => todo!(),
    }
}

const CMD_COMMAND: &str = "COMMAND";
fn parse_command_command(_: &[Value]) -> Command {
    Command::Command()
}

pub struct RESPHandler {
    pub stream: BufReader<monoio::net::TcpStream>,
}

// Handle parsing for the Redis serialization protocol (RESP)
impl RESPHandler {
    // pub async fn decode_command(&mut self) -> Result<Command, std::io::Error> {
    pub async fn decode_command(&mut self) -> Result<Command, std::io::Error> {
        let buffer = self.stream.fill_buf().await.unwrap();
        if buffer.is_empty() {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "empty buffer"));
        }
        let (remaining_buffer, val) = parse(buffer).unwrap();
        let args = match val {
            Value::HashableValue(_) => todo!(),
            Value::NonHashableValue(non_hashable_value) => match non_hashable_value {
                NonHashableValue::Array(vec) => vec,
                _ => todo!(),
            },
            Value::Null => todo!(),
        };

        let blob = match &args[0] {
            Value::HashableValue(hashable_value) => match hashable_value {
                HashableValue::Blob(vec) => vec,
                _ => todo!(),
            },
            Value::NonHashableValue(_) => todo!(),
            Value::Null => todo!(),
        };

        let cmd = match str::from_utf8(blob).unwrap() {
            CMD_HELLO => parse_hello_command(&args),
            CMD_CLIENT => parse_client_command(&args),
            CMD_SET => parse_set_command(&args),
            CMD_GET => parse_get_command(&args),
            CMD_CLUSTER => parse_cluster_command(&args),
            CMD_COMMAND => parse_command_command(&args),
            unsuported_cmd => panic!("Command not supported: {}", unsuported_cmd),
        };

        // println!("Command: {:?}", cmd);
        let consummed_buffer_length = buffer.len() - remaining_buffer.len();
        // println!("consommed buffer size: {}", consummed_buffer_length);
        self.stream.consume(consummed_buffer_length);

        Ok(cmd)
    }

    pub async fn write_resp(&mut self, buff: Vec<u8>) {
        let (res, _) = self.stream.write_all(buff).await;
        res.unwrap();
    }
}
