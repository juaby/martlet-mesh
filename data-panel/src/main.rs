//! 高性能代理，用于调解服务网格中所有服务的入站和出站流量。
//! 它支持动态服务发现、负载均衡、TLS 终止、HTTP/2 和 gPRC 代理、熔断、健康检查、故障注入和性能测量等丰富的功能。
//! Envoy 以 sidecar 的方式部署在相关的服务的 Pod 中，从而无需重新构建或重写代码

#![warn(rust_2018_idioms)]

#[macro_use]
extern crate lazy_static;

use tokio::net::TcpListener;

use std::env;
use std::error::Error;

mod protocol;
mod parser;
mod handler;
mod server;
mod session;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3306".to_string());

    let mut listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // After getting a new connection first we see a clone of the database
                // being created, which is creating a new reference for this connected
                // client to use.

                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.

                    server::handle(socket).await;
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}