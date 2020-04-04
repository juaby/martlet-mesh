use crate::protocol::database::CommandPacketType;

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

/**
 * Command packet type for MySQL.
 */
pub enum MySQLCommandPacketType {

    /**
     * COM_SLEEP.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-sleep.html">COM_SLEEP</a>
     */
    ComSleep = 0x00,
    /**
     * COM_QUIT.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-quit.html">COM_QUIT</a>
     */
    ComQuit = 0x01,
    /**
     * COM_INIT_DB.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-init-db.html">COM_INIT_DB</a>
     */
    ComInitDb = 0x02,
    /**
     * COM_QUERY.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-sleep.html#packet-COM_QUERY">COM_QUERY</a>
     */
    ComQuery = 0x03,
    /**
     * COM_FIELD_LIST.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-sleep.html#packet-COM_FIELD_LIST">COM_FIELD_LIST</a>
     */
    ComFieldList = 0x04,
    /**
     * COM_CREATE_DB.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-sleep.html#packet-COM_CREATE_DB">COM_CREATE_DB</a>
     */
    ComCreateDb = 0x05,
    /**
     * COM_DROP_DB.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-create-db.html">COM_DROP_DB</a>
     */
    ComDropDb = 0x06,
    /**
     * COM_REFRESH.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-refresh.html">COM_REFRESH</a>
     */
    ComRefresh = 0x07,
    /**
     * COM_SHUTDOWN.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-shutdown.html">COM_SHUTDOWN</a>
     */
    ComShutdown = 0x08,
    /**
     * COM_STATISTICS.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-statistics.html#packet-COM_STATISTICS">COM_STATISTICS</a>
     */
    ComStatistics = 0x09,
    /**
     * COM_PROCESS_INFO.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-process-info.html">COM_PROCESS_INFO</a>
     */
    ComProcessInfo = 0x0a,
    /**
     * COM_CONNECT.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-connect.html">COM_CONNECT</a>
     */
    ComConnect = 0x0b,
    /**
     * COM_PROCESS_KILL.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-process-kill.html">COM_PROCESS_KILL</a>
     */
    ComProcessKill = 0x0c,
    /**
     * COM_DEBUG.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-debug.html">COM_DEBUG</a>
     */
    ComDebug = 0x0d,
    /**
     * COM_PING.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-ping.html">COM_PING</a>
     */
    ComPing = 0x0e,
    /**
     * COM_TIME.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-time.html">COM_TIME</a>
     */
    ComTime = 0x0f,
    /**
     * COM_DELAYED_INSERT.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-time.html">COM_DELAYED_INSERT</a>
     */
    ComDelayedInsert = 0x10,
    /**
     * COM_CHANGE_USER.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-sleep.html#packet-COM_CHANGE_USER">COM_CHANGE_USER</a>
     */
    ComChangeUser = 0x11,
    /**
     * COM_BINLOG_DUMP.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-change-user.html">COM_BINLOG_DUMP</a>
     */
    ComBinlogDump = 0x12,
    /**
     * COM_TABLE_DUMP.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-table-dump.html">COM_TABLE_DUMP</a>
     */
    ComTableDump = 0x13,
    /**
     * COM_CONNECT_OUT.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-connect-out.html">COM_CONNECT_OUT</a>
     */
    ComConnectOut = 0x14,
    /**
     * COM_REGISTER_SLAVE.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-register-slave.html">COM_REGISTER_SLAVE</a>
     */
    ComRegisterSlave = 0x15,
    /**
     * COM_STMT_PREPARE.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html">COM_STMT_PREPARE</a>
     */
    ComStmtPrepare = 0x16,
    /**
     * COM_STMT_EXECUTE.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-execute.html">COM_STMT_EXECUTE</a>
     */
    ComStmtExecute = 0x17,
    /**
     * COM_STMT_SEND_LONG_DATA.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html">COM_STMT_SEND_LONG_DATA</a>
     */
    ComStmtSendLongData = 0x18,
    /**
     * COM_STMT_CLOSE.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-close.html">COM_STMT_CLOSE</a>
     */
    ComStmtClose = 0x19,
    /**
     * COM_STMT_RESET.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-reset.html">COM_STMT_RESET</a>
     */
    ComStmtReset = 0x1a,
    /**
     * COM_SET_OPTION.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-set-option.html">COM_SET_OPTION</a>
     */
    ComSetOption = 0x1b,
    /**
     * COM_STMT_FETCH.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html">COM_STMT_FETCH</a>
     */
    ComStmtFetch = 0x1c,
    /**
     * COM_DAEMON.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-daemon.html">COM_DAEMON</a>
     */
    ComDaemon = 0x1d,
    /**
     * COM_BINLOG_DUMP_GTID.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html">COM_BINLOG_DUMP_GTID</a>
     */
    ComBinlogDumpGtid = 0x1e,
    /**
     * COM_RESET_CONNECTION.
     *
     * @see <a href="https://dev.mysql.com/doc/internals/en/com-reset-connection.html">COM_RESET_CONNECTION</a>
     */
    ComResetConnection = 0x1f,

}

impl CommandPacketType for MySQLCommandPacketType {
    fn value_of(t: u8) -> Self {
        match t {
            0x00 => MySQLCommandPacketType::ComSleep,
            0x01 => MySQLCommandPacketType::ComQuit,
            0x02 => MySQLCommandPacketType::ComInitDb,
            0x03 => MySQLCommandPacketType::ComQuery,
            0x04 => MySQLCommandPacketType::ComFieldList,
            0x05 => MySQLCommandPacketType::ComCreateDb,
            0x06 => MySQLCommandPacketType::ComDropDb,
            0x07 => MySQLCommandPacketType::ComRefresh,
            0x08 => MySQLCommandPacketType::ComShutdown,
            0x09 => MySQLCommandPacketType::ComStatistics,
            0x0a => MySQLCommandPacketType::ComProcessInfo,
            0x0b => MySQLCommandPacketType::ComConnect,
            0x0c => MySQLCommandPacketType::ComProcessKill,
            0x0d => MySQLCommandPacketType::ComDebug,
            0x0e => MySQLCommandPacketType::ComPing,
            0x0f => MySQLCommandPacketType::ComTime,
            0x10 => MySQLCommandPacketType::ComDelayedInsert,
            0x11 => MySQLCommandPacketType::ComChangeUser,
            0x12 => MySQLCommandPacketType::ComBinlogDump,
            0x13 => MySQLCommandPacketType::ComTableDump,
            0x14 => MySQLCommandPacketType::ComConnectOut,
            0x15 => MySQLCommandPacketType::ComRegisterSlave,
            0x16 => MySQLCommandPacketType::ComStmtPrepare,
            0x17 => MySQLCommandPacketType::ComStmtExecute,
            0x18 => MySQLCommandPacketType::ComStmtSendLongData,
            0x19 => MySQLCommandPacketType::ComStmtClose,
            0x1a => MySQLCommandPacketType::ComStmtReset,
            0x1b => MySQLCommandPacketType::ComSetOption,
            0x1c => MySQLCommandPacketType::ComStmtFetch,
            0x1d => MySQLCommandPacketType::ComDaemon,
            0x1e => MySQLCommandPacketType::ComBinlogDumpGtid,
            0x1f => MySQLCommandPacketType::ComResetConnection,
            _ => {
                panic!("unsupport command query type!!!")
            }
        }
    }

}