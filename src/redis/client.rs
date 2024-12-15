use std::borrow::Cow;

use monoio::{io::BufReader, net::TcpStream};

use crate::topology::{ReactorMetadata, Topology};

use super::{
    command::RESPHandler,
    resp::{HashableValue, NonHashableValue, Value},
    serde::ToResp,
};

pub struct Client {
    handler: RESPHandler,
}

impl Client {
    pub async fn new(addr: String) -> Client {
        let stream = BufReader::new(TcpStream::connect(addr).await.unwrap());
        Client {
            handler: RESPHandler { stream },
        }
    }

    pub async fn cluster_join(&mut self, reactors: Vec<ReactorMetadata>) -> Topology {
        let metadata: Vec<Value> = reactors.iter().map(|rm| rm.to_resp()).collect();

        let request = Value::NonHashableValue(NonHashableValue::Array(vec![
            Value::HashableValue(HashableValue::String(Cow::from("CLUSTER"))),
            Value::HashableValue(HashableValue::String(Cow::from("JOIN"))),
            Value::NonHashableValue(NonHashableValue::Array(metadata)),
        ]))
        .to_bytes();

        self.handler.write_resp(request);
        self.handler.decode_response::<Topology>().await.unwrap()
    }
}
