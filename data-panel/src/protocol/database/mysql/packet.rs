use crate::protocol::database::mysql::constant::{MySQLStatusFlag, PROTOCOL_VERSION, SERVER_VERSION, CHARSET, MySQLCapabilityFlag, NUL, SEED};
use crate::protocol::database::{PacketPayload, DatabasePacket};

use rand::Rng;
use bytes::{BytesMut, Buf, BufMut, Bytes};

use std::convert::TryInto;

const PAYLOAD_LENGTH: u32 = 3;
const SEQUENCE_LENGTH: u32 = 1;

/// Generate random bytes.
///
/// @param length length for generated bytes.
/// @return generated bytes
///
pub fn generate_random_bytes(len: u32, seed: &mut Vec<u8>) -> Vec<u8> {
    let mut random = rand::thread_rng();
    for _i in 0..len {
        seed.push(SEED[random.gen_range(0, SEED.len())]);
    }
    seed.to_vec()
}

/**
 * MySQL payload operation for MySQL packet data types.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/describing-packets.html">describing packets</a>
 */

pub struct MySQLPacketPayload {

    bytes_mut: BytesMut,

}

impl MySQLPacketPayload {

    pub fn new() -> Self {
        MySQLPacketPayload {
            bytes_mut: BytesMut::new()
        }
    }

    pub fn new_with_payload(bytes_mut: BytesMut) -> Self {
        MySQLPacketPayload {
            bytes_mut: bytes_mut
        }
    }

    pub fn put_u8(&mut self, val: u8) {
        self.bytes_mut.put_u8(val);
    }

    pub fn put_u16_le(&mut self, val: u16) {
        self.bytes_mut.put_u16_le(val);
    }

    pub fn put_u32_le(&mut self, val: u32) {
        self.bytes_mut.put_u32_le(val);
    }

    pub fn put_slice(&mut self, val: &[u8]) {
        self.bytes_mut.put_slice(val);
    }

    pub fn put_string_with_nul(&mut self, val: &[u8]) {
        self.bytes_mut.put_slice(val);
        self.bytes_mut.put_u8(NUL);
    }

    pub fn get_uint_le(&mut self, n: usize) -> u64 {
        self.bytes_mut.get_uint_le(n)
    }

    pub fn get_uint(&mut self, n: usize) -> u64 {
        self.bytes_mut.get_uint(n)
    }

    pub fn advance(&mut self, n: usize) {
        self.bytes_mut.advance(n);
    }

    // string with nul
    pub fn get_string_nul(&mut self) -> String {
        let pos = match self.bytes_mut.bytes().iter().position(|&x| x == 0) {
            Some(pos) => pos,
            None => 0 // TODO
        };
        let bytes = self.bytes_mut.split_to(pos);
        let result = String::from_utf8_lossy(bytes.bytes()).to_string();
        self.bytes_mut.advance(1);
        result
    }

    /**
     * Write lenenc integer to byte buffers.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
     *
     * @param value lenenc integer
     */
    pub fn put_int_lenenc(&mut self, v: usize) {
        let two: u64 = 2;
        if v < 0xfb {
            self.bytes_mut.put_u8(v as u8);
        } else if v < two.pow(16).try_into().unwrap() {
            self.bytes_mut.put_u8(0xfc);
            self.bytes_mut.put_u16_le(v as u16);
        } else if v < two.pow(24).try_into().unwrap() {
            self.bytes_mut.put_u8(0xfd);
            self.bytes_mut.put_uint_le(v as u64, 3);
        } else {
            self.bytes_mut.put_u8(0xfe);
            self.bytes_mut.put_u64_le(v as u64);
        }
    }

    /**
     * Read lenenc integer from byte buffers.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger">LengthEncodedInteger</a>
     *
     * @return lenenc integer
     */
    pub fn get_int_lenenc(&mut self) -> u64 {
        let first_byte = self.bytes_mut.get_uint(1) & 0xff;
        if first_byte < 0xfb {
            first_byte
        } else if 0xfb == first_byte {
            0
        } else if 0xfc == first_byte {
            self.bytes_mut.get_uint_le(2)
        } else if 0xfd == first_byte {
            self.bytes_mut.get_uint_le(3)
        } else {
            self.bytes_mut.get_uint_le(8)
        }
    }

    /**
     * Read lenenc string from byte buffers for bytes.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
     *
     * @return lenenc bytes
     */
    pub fn get_string_lenenc(&mut self) -> Vec<u8> {
        let length = self.get_int_lenenc() as u32;
        let tmp = self.bytes_mut.split_to(length as usize);
        tmp.to_vec()
    }

    /**
    * Read fixed length string from byte buffers and return bytes.
    *
    * @see <a href="https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString">FixedLengthString</a>
    *
    * @param length length of fixed string
    *
    * @return fixed length bytes
    */
    pub fn get_string_fix(&mut self) -> Vec<u8> {
        let length = self.bytes_mut.get_uint(1) as u32 & 0xff;
        let tmp = self.bytes_mut.split_to(length as usize);
        tmp.to_vec()
    }

}

impl PacketPayload for MySQLPacketPayload {

    fn get_bytes(&mut self) -> Bytes {
        self.bytes_mut.to_bytes()
    }

}

/**
 * Handshake packet protocol for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake">Handshake</a>
 */
pub trait MySQLPacket {

    /**
     * Get sequence ID.
     *
     * @return sequence ID
     */
    fn get_sequence_id(&self) -> u32;

}

pub struct MySQLHandshakePacket {

    protocol_version: u8,
    server_version: String,
    thread_id: u32,
    capability_flags_lower: u32,
    character_set: u8,
    status_flag: u32,
    seed1: Vec<u8>,
    seed2: Vec<u8>,
    capability_flags_upper: u32,
    auth_plugin_name: String,

}

impl MySQLHandshakePacket {

    pub fn new(thread_id: u32) -> Self {
        let mut seed1: Vec<u8> = Vec::new();
        let mut seed2: Vec<u8> = Vec::new();
        let seed1= generate_random_bytes(8, seed1.as_mut());
        let seed2= generate_random_bytes(12, seed2.as_mut());

        let mut capability_flags_lower: u32 = 0; // capability_flags_lower
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientLongPassword as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientFoundRows as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientLongFlag as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientConnectWithDb as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientOdbc as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientIgnoreSpace as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientProtocol41 as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientInteractive as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientIgnoreSigpipe as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientTransactions as u32;
        capability_flags_lower = capability_flags_lower | MySQLCapabilityFlag::ClientSecureConnection as u32;

        MySQLHandshakePacket {
            protocol_version: PROTOCOL_VERSION,
            server_version: SERVER_VERSION.to_string(),
            thread_id: thread_id,
            capability_flags_lower: capability_flags_lower,
            character_set: CHARSET,
            status_flag: MySQLStatusFlag::ServerStatusAutocommit as u32,
            seed1: seed1,
            seed2: seed2,
            capability_flags_upper: 0,
            auth_plugin_name: "".to_string()
        }
    }

}

impl MySQLPacket for MySQLHandshakePacket {

    fn get_sequence_id(&self) -> u32 {
        0
    }

}

impl DatabasePacket<MySQLPacketPayload> for MySQLHandshakePacket {

    fn encode<'p,'d>(this: &'d mut Self, payload: &'p mut MySQLPacketPayload) -> &'p mut MySQLPacketPayload {
        payload.put_u8(this.get_sequence_id() as u8); // seq
        payload.put_u8(this.protocol_version); // protocol version
        payload.put_string_with_nul(this.server_version.as_bytes()); // server version
        payload.put_u32_le(this.thread_id); //thread id
        payload.put_string_with_nul(this.seed1.as_slice()); //seed 1
        payload.put_u16_le(this.capability_flags_lower as u16); // capability_flags_lower
        payload.put_u8(this.character_set); // charset
        payload.put_u16_le(this.status_flag as u16); // server status
        //capability_flags_upper = capability_flags_upper | (MySQLCapabilityFlag::ClientPluginAuth as u32);
        payload.put_u16_le(this.capability_flags_upper as u16); // capability_flags_upper
        // isClientPluginAuth
        // seed len
        if 0 != ((this.capability_flags_upper << 16) & (MySQLCapabilityFlag::ClientPluginAuth as u32)) {
            payload.put_u8((this.seed1.len() + this.seed2.len()) as u8);
        } else {
            payload.put_u8(0);
        }
        // Write null for reserved to byte buffers.
        let reserved: [u8; 10] = [0,0,0,0,0,0,0,0,0,0];
        payload.put_slice(&reserved);
        // isClientSecureConnection
        // seed 2
        if 0 != (this.capability_flags_lower & (MySQLCapabilityFlag::ClientSecureConnection as u32)) {
            payload.put_string_with_nul(this.seed2.as_slice());
        }
        // isClientPluginAuth
        // auth_plugin_name
        if 0 != ((this.capability_flags_upper << 16) & (MySQLCapabilityFlag::ClientPluginAuth as u32)) {
            payload.put_string_with_nul(this.auth_plugin_name.as_bytes());
        }

        payload
    }

}

/**
 * Handshake response above MySQL 4.1 packet protocol.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41">HandshakeResponse41</a>
 */

pub struct MySQLHandshakeResponse41Packet {

    sequence_id: u32,
    max_packet_size: u32,
    character_set: u8,
    user_name: String,
    auth_response: Vec<u8>,
    capability_flags: u32,
    database: String,
    auth_plugin_name: String,

}

impl MySQLHandshakeResponse41Packet {

    pub fn new() -> Self {
        MySQLHandshakeResponse41Packet {
            sequence_id: 0,
            max_packet_size: 0,
            character_set: 0,
            user_name: "".to_string(),
            auth_response: vec![],
            capability_flags: 0,
            database: "".to_string(),
            auth_plugin_name: "".to_string()
        }
    }

}

impl DatabasePacket<MySQLPacketPayload> for MySQLHandshakeResponse41Packet {

    fn decode(&mut self, payload: &mut MySQLPacketPayload) {
        let len = payload.get_uint_le(3);
        self.sequence_id = payload.get_uint(1) as u32 & 0xff;
        self.capability_flags = payload.get_uint_le(4) as u32;
        self.max_packet_size = payload.get_uint_le(4) as u32;
        self.character_set = payload.get_uint(1) as u8 & 0xff;
        payload.advance(23);

        // string with nul
        self.user_name = payload.get_string_nul();

        self.auth_response = if 0 != (self.capability_flags & MySQLCapabilityFlag::ClientPluginAuthLenencClientData as u32) {
            payload.get_string_lenenc()
        } else if 0 != (self.capability_flags & MySQLCapabilityFlag::ClientSecureConnection as u32) {
            payload.get_string_fix()
        } else {
            let auth = payload.get_string_nul();
            auth.into_bytes()
        };

        self.database = if 0 != (self.capability_flags & MySQLCapabilityFlag::ClientConnectWithDb as u32) {
            payload.get_string_nul()
        } else {
            String::from("")
        };

        self.auth_plugin_name = if 0 != (self.capability_flags & MySQLCapabilityFlag::ClientPluginAuth as u32) {
            payload.get_string_nul()
        } else {
            String::from("")
        };
    }

}

impl MySQLPacket for MySQLHandshakeResponse41Packet {

    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }

}

/**
 * OK packet protocol for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html">OK Packet</a>
 */
pub struct MySQLOKPacket {

    /**
     * Header of OK packet.
     */
    header: u8,
    sequence_id: u32,
    affected_rows: u64,
    last_insert_id: u64,
    status_flag: u32,
    warnings: u32,
    info: String,

}

impl MySQLOKPacket {

    pub fn new(sequence_id: u32) -> Self {
        MySQLOKPacket {
            header: 0x00,
            sequence_id: sequence_id,
            affected_rows: 0,
            last_insert_id: 0,
            status_flag: MySQLStatusFlag::ServerStatusAutocommit as u32,
            warnings: 0,
            info: "".to_string()
        }
    }

}

impl DatabasePacket<MySQLPacketPayload> for MySQLOKPacket {

    fn encode<'p,'d>(this: &'d mut Self, payload: &'p mut MySQLPacketPayload) -> &'p mut MySQLPacketPayload {
        payload.put_u8(this.get_sequence_id() as u8); // seq
        payload.put_u8(this.header);

        payload.put_int_lenenc(this.affected_rows as usize);
        payload.put_int_lenenc(this.last_insert_id as usize);

        payload.put_u16_le(this.status_flag as u16);
        payload.put_u16_le(this.warnings as u16);

        payload.put_slice(this.info.as_bytes());

        payload
    }

}

impl MySQLPacket for MySQLOKPacket {

    fn get_sequence_id(&self) -> u32 {
        self.sequence_id
    }

}