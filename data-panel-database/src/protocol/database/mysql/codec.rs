use tokio::io::{AsyncWrite, AsyncRead};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::LengthDelimitedCodec;
use data_panel_common::service::ServiceCodec;

pub struct MySQLCodec {}
impl ServiceCodec for MySQLCodec {
    fn write_frame<T: AsyncWrite>(&self, io: T) -> FramedWrite<T, LengthDelimitedCodec> {
        LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(3)
            .length_adjustment(1)
            .little_endian()
            .num_skip(0)
            .new_write(io)
    }

    fn read_frame<T: AsyncRead>(&self, io: T) -> FramedRead<T, LengthDelimitedCodec> {
        LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(3)
            .length_adjustment(4)
            .little_endian()
            .num_skip(0)
            .new_read(io)
    }
}