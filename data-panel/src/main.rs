//! A high-performance broker that mediates inbound and outbound traffic for all services in the service grid.
//! It supports dynamic service discovery, load balancing, TLS, termination, HTTP/2 and GPRC proxies, fuse-outs, health checks, fault injection, and performance measurements.
//! The Martlet is deployed in the POD of the associated service in a sidecar manner, eliminating the need to rebuild or rewrite the code

#![warn(rust_2018_idioms)]

#[macro_use]
extern crate bitflags;

#[macro_use]
extern crate lazy_static;

use std::error::Error;
use std::fs::File;
use std::io::Read;
use yaml_rust::{YamlLoader, YamlEmitter};
use clap::{App, Arg, SubCommand};

mod protocol;
mod parser;
mod handler;
mod server;
mod session;
mod discovery;
mod common;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("Database Mesh")
        .version("0.1.0")
        .author("AlphaPo")
        .about("A Joy Database Mesh, Maybe General Service Mesh!")
        .arg(Arg::with_name("verbose")
            .short("v")
            .multiple(true)
            .help("verbosity level"))
        .args_from_usage("-c, --config=[FILE] 'Target file you want to change'")
        .subcommand(SubCommand::with_name("test")
            .about("does testing things")
            .arg_from_usage("-l, --list 'lists test values'"))
        .get_matches();

    if let Some(f) = matches.value_of("config-path") {
        println!("path : {}", f);
    }

    if let Some(matches) = matches.subcommand_matches("test") {
        if matches.is_present("list") {
            println!("Printing testing lists...");
        } else {
            println!("Not printing testing lists...");
        }
    }

    let mut file = File::open("./data-panel/etc/app.yaml").expect("Unable to open file");
    let mut contents = String::new();

    file.read_to_string(&mut contents)
        .expect("Unable to read file");

    let docs = YamlLoader::load_from_str(contents.as_str()).unwrap();

    // Multi document support, doc is a yaml::Yaml
    let doc = &docs[0];

    // Debug support
    println!("{:?}", doc);

    // Index access for map & array
    assert_eq!(doc["app"]["name"].as_str().unwrap(), "Database Mesh");
    assert_eq!(doc["system"]["timeout"].as_i64().unwrap(), 5000);

    // Chained key/array access is checked and won't panic,
    // return BadValue if they are not exist.
    assert!(doc["INVALID_KEY"][100].is_badvalue());

    // Dump the YAML object
    let mut out_str = String::new();
    {
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(doc).unwrap(); // dump the YAML object to a String
    }
    println!("{}", out_str);

    server::service::serve().await
}