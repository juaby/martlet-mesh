use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::io::{AsyncWrite, AsyncRead};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;

use async_trait::async_trait;
use crate::service::mysql::MySQLService;

pub mod mysql;