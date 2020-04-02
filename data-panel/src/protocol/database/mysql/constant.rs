/// Protocol version is always 0x0A.
pub const PROTOCOL_VERSION: u8 = 0x0A;

/// String with NUL
pub const NUL: u8 = 0x00;

/// Server version.
pub const SERVER_VERSION: &str = "5.6.4-Sharding-Proxy 5.0.0-RC1";

/// Charset code 0x21 is utf8_general_ci.
pub const CHARSET: u8 = 0x21;

/// Status flags are a bit-field for MySQL.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/status-flags.html#packet-Protocol::StatusFlags">StatusFlags</a>
///
pub enum MySQLStatusFlag {

    ServerStatusInTrans = 0x0001,
    ServerStatusAutocommit = 0x0002,
    ServerMoreResultsExists = 0x0008,
    ServerStatusNoGoodIndexUsed = 0x0010,
    ServerStatusNoIndexUsed = 0x0020,
    ServerStatusCursorExists = 0x0040,
    ServerStatusLastRowSent = 0x0080,
    ServerStatusDbDropped = 0x0100,
    ServerStatusNoBackslashEscapes = 0x0200,
    ServerStatusMetadataChanged = 0x0400,
    ServerQueryWasSlow = 0x0800,
    ServerPsOutParams = 0x1000,
    ServerStatusInTransReadonly = 0x2000,
    ServerSessionStateChanged = 0x4000,
}

///
/// Capability flag for MySQL.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags">CapabilityFlags</a>
///
pub enum MySQLCapabilityFlag {

    ClientLongPassword = 0x00000001,
    ClientFoundRows = 0x00000002,
    ClientLongFlag = 0x00000004,
    ClientConnectWithDb = 0x00000008,
    ClientNoSchema = 0x00000010,
    ClientCompress = 0x00000020,
    ClientOdbc = 0x00000040,
    ClientLocalFiles = 0x00000080,
    ClientIgnoreSpace = 0x00000100,
    ClientProtocol41 = 0x00000200,
    ClientInteractive = 0x00000400,
    ClientSsl = 0x00000800,
    ClientIgnoreSigpipe = 0x00001000,
    ClientTransactions = 0x00002000,
    ClientReserved = 0x00004000,
    ClientSecureConnection = 0x00008000,
    ClientMultiStatements = 0x00010000,
    ClientMultiResults = 0x00020000,
    ClientPsMultiResults = 0x00040000,
    ClientPluginAuth = 0x00080000,
    ClientConnectAttrs = 0x00100000,
    ClientPluginAuthLenencClientData = 0x00200000,
    ClientCanHandleExpiredPasswords = 0x00400000,
    ClientSessionTrack = 0x00800000,
    ClientDeprecateEof = 0x01000000,
}

///
pub const SEED: &[u8] = b"abedefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

///
/// MySQL client/server protocol Authentication Method.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/authentication-method.html">Authentication Method</a>
///
pub enum MySQLAuthenticationMethod {
    OldPasswordAuthentication,
    SecurePasswordAuthentication,
    ClearTextAuthentication,
    WindowsNativeAuthentication,
    SHA256,
}

impl MySQLAuthenticationMethod {

    pub fn value(&self) -> &str {
        match *self {
            MySQLAuthenticationMethod::OldPasswordAuthentication => "mysql_old_password",
            MySQLAuthenticationMethod::SecurePasswordAuthentication => "mysql_native_password",
            MySQLAuthenticationMethod::ClearTextAuthentication => "mysql_clear_password",
            MySQLAuthenticationMethod::WindowsNativeAuthentication => "authentication_windows_client",
            MySQLAuthenticationMethod::SHA256 => "sha256_password",
        }
    }

}

///
/// Column types for MySQL.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type">Column Type</a>
/// @see <a href="https://github.com/apache/incubator-shardingsphere/issues/4355"></a>
///
pub enum MySQLColumnType {

    MysqlTypeDecimal = 0x00,
    MysqlTypeTiny = 0x01,
    MysqlTypeShort = 0x02,
    MysqlTypeLong = 0x03,
    MysqlTypeFloat = 0x04,
    MysqlTypeDouble = 0x05,
    MysqlTypeNull = 0x06,
    MysqlTypeTimestamp = 0x07,
    MysqlTypeLonglong = 0x08,
    MysqlTypeInt24 = 0x09,
    MysqlTypeDate = 0x0a,
    MysqlTypeTime = 0x0b,
    MysqlTypeDatetime = 0x0c,
    MysqlTypeYear = 0x0d,
    MysqlTypeNewDate = 0x0e,
    MysqlTypeVarchar = 0x0f,
    MysqlTypeBit = 0x10,
    MysqlTypeTimestamp2 = 0x11,
    MysqlTypeDatetime2 = 0x12,
    MysqlTypeTime2 = 0x13,
    MysqlTypeNewDecimal = 0xf6,
    MysqlTypeEnum = 0xf7,
    MysqlTypeSet = 0xf8,
    MysqlTypeTinyBlob = 0xf9,
    MysqlTypeMediumBlob = 0xfa,
    MysqlTypeLongBlob = 0xfb,
    MysqlTypeBlob = 0xfc,
    MysqlTypeVarString = 0xfd,
    MysqlTypeString = 0xfe,
    MysqlTypeGeometry = 0xff,

}

///
/// New parameters bound flag for MySQL.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-execute.html">COM_STMT_EXECUTE</a>
///
pub enum MySQLNewParametersBoundFlag {

    ParameterTypeExist = 1,
    ParameterTypeNotExist = 0,

}