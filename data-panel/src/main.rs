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

use clap::{App, Arg, SubCommand};
use toml::Value;
use yaml_rust::{YamlEmitter, YamlLoader};

use data_panel_common::config::config::MeshConfig;

mod protocol;
mod handler;
mod service;
mod session;
mod discovery;
mod common;
mod config;

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

    println!("args : {:#?}" ,matches);

    let config_path = matches.value_of("config").unwrap_or("./etc/app.toml");
    println!("config_path : {}", config_path);

    if let Some(matches) = matches.subcommand_matches("test") {
        if matches.is_present("list") {
            println!("Printing testing lists...");
        } else {
            println!("Not printing testing lists...");
        }
    }

    let mut file = File::open(config_path).expect("Unable to open file");
    let mut contents = String::new();

    file.read_to_string(&mut contents).expect("Unable to read file");

    let docs = contents.as_str().parse::<Value>().unwrap();

    // Multi document support, doc is a yaml::Yaml
    let doc = &docs;

    // Debug support
    println!("{:?}", doc);

    // Index access for map & array
    assert_eq!(doc["app"]["name"].as_str().unwrap(), "Database Mesh");
    assert_eq!(doc["system"]["timeout"].as_integer().unwrap(), 5000);

    // Chained key/array access is checked and won't panic,
    // return BadValue if they are not exist.
    // assert!(doc["INVALID_KEY"][100].is_badvalue());

    println!("{}", toml::to_string(doc).unwrap());

    let mesh_config = MeshConfig::from_file(config_path);
    mesh_config.make_current();

    println!("{:#?}", MeshConfig::current());

    let service = service::new_service();

    service.serve().await
}