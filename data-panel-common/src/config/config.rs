use std::fs::File;
use std::io::Read;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MeshConfig {
    app: AppConfig,
    control: ControlConfig,
    system: SystemConfig,
}

impl MeshConfig {
    pub fn from_str(config_str: &str) -> Self {
        toml::from_str(config_str).unwrap()
    }

    pub fn from_file(config_file: &str) -> Self {
        let mut file = File::open(config_file).expect("Unable to open file");
        let mut config_str = String::new();
        file.read_to_string(&mut config_str).expect("Unable to read file");
        Self::from_str(&*config_str)
    }
}

impl MeshConfig {
    pub fn get_host() -> String {
        MeshConfig::current().app.host.clone()
    }

    pub fn get_port() -> u32 {
        MeshConfig::current().app.port
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AppConfig {
    name: String,
    host: String,
    port: u32,
    version: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ControlConfig {
    pilot: String,
    mixer: String,
    citadel: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SystemConfig {
    timeout: u32,
}

impl MeshConfig {
    pub fn current() -> Arc<MeshConfig> {
        MESH_CONFIG_CACHE.read().unwrap().clone()
    }

    pub fn make_current(self) {
        *MESH_CONFIG_CACHE.write().unwrap() = Arc::new(self)
    }
}

lazy_static! {
    static ref MESH_CONFIG_CACHE: RwLock<Arc<MeshConfig>> = RwLock::new(Default::default());
}