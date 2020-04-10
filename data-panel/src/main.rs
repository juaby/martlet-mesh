//! 高性能代理，用于调解服务网格中所有服务的入站和出站流量。
//! 它支持动态服务发现、负载均衡、TLS 终止、HTTP/2 和 gPRC 代理、熔断、健康检查、故障注入和性能测量等丰富的功能。
//! Envoy 以 sidecar 的方式部署在相关的服务的 Pod 中，从而无需重新构建或重写代码

#![warn(rust_2018_idioms)]

#[macro_use]
extern crate lazy_static;

use std::error::Error;

mod protocol;
mod parser;
mod handler;
mod server;
mod session;
mod discovery;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    server::service::serve().await
}