use bytes::Bytes;

pub mod mysql;
pub mod postgresql;

/**
 * Packet payload.
 */
pub trait PacketPayload {
    /**
     * Get byte buf.
     *
     * @return byte buf
     */
    fn get_payload(&mut self) -> Bytes;
}

/**
 * Database packet.
 *
 * @param <T> type of packet payload
 */
pub trait DatabasePacket<H, T: PacketPayload, Session> {
    /**
     * Write packet to byte buffer.
     *
     * @param payload packet payload to be written
     */
    fn encode<'p, 'd>(this: &'d mut Self, payload: &'p mut T) -> &'p mut T {
        payload
    }

    /**
     * Read packet from byte buffer.
     *
     * @param payload packet payload to be written
     */
    fn decode<'p, 'd>(this: &'d mut Self, header: &'p H, payload: &'p mut T, session_ctx: &mut Session) -> &'d mut Self { this }
}

/**
 * Command packet type.
 */
pub trait CommandPacketType {
    fn value_of(t: u8) -> Self;
}