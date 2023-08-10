use monoio::{net::TcpListener, io::BufReader};
use monoio_compat::TcpStreamCompat;
use tokio::net::TcpStream;

use crate::{
    memcached::{MemcachedBinaryHandler, Response},
    storageproxy::StorageProxy,
};

pub struct MemcachedBinaryServer {
    pub host_port: String,
    pub storage_proxy: StorageProxy,
}

impl MemcachedBinaryServer {
    pub async fn listen(self) {
        let listener = TcpListener::bind(self.host_port.clone()).unwrap();

        println!("Listening on {}", listener.local_addr().unwrap());
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let storage_proxy = self.storage_proxy.clone();
            let reader = BufReader::new(stream);
            monoio::spawn(async move {
                let mut handler = MemcachedBinaryHandler { stream: reader };
                // let compat = TcpStreamCompat::new(stream);
                // let tokio_stream: TcpStream = compat.into();
                // compat.poll_peek();
                loop {
                    // if handler.await_new_data().await.is_err() {
                    //     return;
                    // }
                    let memcached_command = match handler.decode_command().await {
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
                    let resp = storage_proxy.dispatch(memcached_command.to_api_command()).await;
                    handler.write_resp(Response::from_api_response(resp).to_bytes()).await;
                }
            });
        }
    }
}
