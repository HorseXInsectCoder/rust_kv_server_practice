
use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        tokio::spawn(async move {
            // 由插件提示看到这里stream的类型是：AsyncProstStream<TcpStream, CommandRequest, CommandResponse, AsyncDestination>
            // TcpStream由stream传入得到, AsyncDestination由for_async调用得到（如果没有调用for_async，那么是SyncDestination）
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(msg)) = stream.next().await {
                info!("Got a new command: {:?}", msg);
                // 创建一个 404 response 返回给客户端
                let mut resp = CommandResponse::default();
                resp.status = 404;
                resp.message = "Not Found".to_string();
                stream.send(resp).await.unwrap();
            }
            info!("Client {:?} disconnectd", addr);
        });
    }
}