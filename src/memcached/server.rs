use glommio::net::TcpListener;

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
            let stream = listener.accept().await.unwrap();
            let storage_proxy = self.storage_proxy.clone();
            glommio::spawn_local(async move {
                let mut handler = MemcachedBinaryHandler { stream };

                loop {
                    if handler.await_new_data().await.is_err() {
                        return;
                    }
                    let memcached_command = handler.decode_command().await.unwrap();
                    let resp = storage_proxy.dispatch(memcached_command.to_api_command()).await;
                    handler.write_resp(&Response::from_api_response(resp).to_bytes()).await;
                }
            })
            .detach();
        }
    }
}
