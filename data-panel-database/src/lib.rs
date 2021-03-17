//! A high-performance broker that mediates inbound and outbound traffic for all services in the service grid.
//! It supports dynamic service discovery, load balancing, TLS, termination, HTTP/2 and GPRC proxies, fuse-outs, health checks, fault injection, and performance measurements.
//! The Martlet is deployed in the POD of the associated service in a sidecar manner, eliminating the need to rebuild or rewrite the code

#![warn(rust_2018_idioms)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate lazy_static;

pub mod protocol;
pub mod handler;
pub mod service;
pub mod session;
pub mod discovery;
pub mod common;
pub mod config;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
