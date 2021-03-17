use crate::protocol::database::CommandPacketType;

/// Protocol version is always 0x0A.
pub const PROTOCOL_VERSION: u8 = 0x0A;

/// String with NUL
pub const NUL: u8 = 0x00;

/// Server version.
pub const SERVER_VERSION: &str = "5.7.29-DBMesh 0.1.0";

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

//
// Capability flag for MySQL.
//
// @see <a href="https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags">CapabilityFlags</a>
//
bitflags! {
    /// Client capability flags
    pub struct MySQLCapabilityFlag: u32 {
        /// Use the improved version of Old Password Authentication. Assumed to be set since 4.1.1.
        const CLIENT_LONG_PASSWORD                  = 0x0000_0001;

        /// Send found rows instead of affected rows in EOF_Packet.
        const CLIENT_FOUND_ROWS                     = 0x0000_0002;

        /// Get all column flags.
        /// Longer flags in Protocol::ColumnDefinition320.
        ///
        /// ### Server
        /// Supports longer flags.
        ///
        /// ### Client
        /// Expects longer flags.
        const CLIENT_LONG_FLAG                      = 0x0000_0004;

        /// Database (schema) name can be specified on connect in Handshake Response Packet.
        /// ### Server
        /// Supports schema-name in Handshake Response Packet.
        ///
        /// ### Client
        /// Handshake Response Packet contains a schema-name.
        const CLIENT_CONNECT_WITH_DB                = 0x0000_0008;

        /// Don't allow database.table.column.
        const CLIENT_NO_SCHEMA                      = 0x0000_0010;

        /// Compression protocol supported.
        ///
        /// ### Server
        /// Supports compression.
        ///
        /// ### Client
        /// Switches to Compression compressed protocol after successful authentication.
        const CLIENT_COMPRESS                       = 0x0000_0020;

        /// Special handling of ODBC behavior.
        const CLIENT_ODBC                           = 0x0000_0040;

        /// Can use LOAD DATA LOCAL.
        ///
        /// ### Server
        /// Enables the LOCAL INFILE request of LOAD DATA|XML.
        ///
        /// ### Client
        /// Will handle LOCAL INFILE request.
        const CLIENT_LOCAL_FILES                    = 0x0000_0080;

        /// Ignore spaces before '('.
        ///
        /// ### Server
        /// Parser can ignore spaces before '('.
        ///
        /// ### Client
        /// Let the parser ignore spaces before '('.
        const CLIENT_IGNORE_SPACE                   = 0x0000_0100;

        const CLIENT_PROTOCOL_41                    = 0x0000_0200;

        /// This is an interactive client.
        /// Use System_variables::net_wait_timeout versus System_variables::net_interactive_timeout.
        ///
        /// ### Server
        /// Supports interactive and noninteractive clients.
        ///
        /// ### Client
        /// Client is interactive.
        const CLIENT_INTERACTIVE                    = 0x0000_0400;

        /// Use SSL encryption for the session.
        ///
        /// ### Server
        /// Supports SSL
        ///
        /// ### Client
        /// Switch to SSL after sending the capability-flags.
        const CLIENT_SSL                            = 0x0000_0800;

        /// Client only flag. Not used.
        ///
        /// ### Client
        /// Do not issue SIGPIPE if network failures occur (libmysqlclient only).
        const CLIENT_IGNORE_SIGPIPE                 = 0x0000_1000;

        /// Client knows about transactions.
        ///
        /// ### Server
        /// Can send status flags in OK_Packet / EOF_Packet.
        ///
        /// ### Client
        /// Expects status flags in OK_Packet / EOF_Packet.
        ///
        /// ### Note
        /// This flag is optional in 3.23, but always set by the server since 4.0.
        const CLIENT_TRANSACTIONS                   = 0x0000_2000;

        const CLIENT_RESERVED                       = 0x0000_4000;

        const CLIENT_SECURE_CONNECTION              = 0x0000_8000;

        /// Enable/disable multi-stmt support.
        /// Also sets CLIENT_MULTI_RESULTS. Currently not checked anywhere.
        ///
        /// ### Server
        /// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
        ///
        /// ### Client
        /// May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
        const CLIENT_MULTI_STATEMENTS               = 0x0001_0000;

        /// Enable/disable multi-results.
        ///
        /// ### Server
        /// Can send multiple resultsets for COM_QUERY. Error if the server needs to send
        /// them and client does not support them.
        ///
        /// ### Client
        /// Can handle multiple resultsets for COM_QUERY.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_MULTI_RESULTS                  = 0x0002_0000;

        /// Multi-results and OUT parameters in PS-protocol.
        ///
        /// ### Server
        /// Can send multiple resultsets for COM_STMT_EXECUTE.
        ///
        /// ### Client
        /// Can handle multiple resultsets for COM_STMT_EXECUTE.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_PS_MULTI_RESULTS               = 0x0004_0000;

        /// Client supports plugin authentication.
        ///
        /// ### Server
        /// Sends extra data in Initial Handshake Packet and supports the pluggable
        /// authentication protocol.
        ///
        /// ### Client
        /// Supports authentication plugins.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_PLUGIN_AUTH                    = 0x0008_0000;

        /// Client supports connection attributes.
        ///
        /// ### Server
        /// Permits connection attributes in Protocol::HandshakeResponse41.
        ///
        /// ### Client
        /// Sends connection attributes in Protocol::HandshakeResponse41.
        const CLIENT_CONNECT_ATTRS                  = 0x0010_0000;

        /// Enable authentication response packet to be larger than 255 bytes.
        /// When the ability to change default plugin require that the initial password
        /// field in the Protocol::HandshakeResponse41 paclet can be of arbitrary size.
        /// However, the 4.1 client-server protocol limits the length of the auth-data-field
        /// sent from client to server to 255 bytes. The solution is to change the type of
        /// the field to a true length encoded string and indicate the protocol change with
        /// this client capability flag.
        ///
        /// ### Server
        /// Understands length-encoded integer for auth response data in
        /// Protocol::HandshakeResponse41.
        ///
        /// ### Client
        /// Length of auth response data in Protocol::HandshakeResponse41 is a
        /// length-encoded integer.
        ///
        /// ### Note
        /// The flag was introduced in 5.6.6, but had the wrong value.
        const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020_0000;

        /// Don't close the connection for a user account with expired password.
        ///
        /// ### Server
        /// Announces support for expired password extension.
        ///
        /// ### Client
        /// Can handle expired passwords.
        const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x0040_0000;

        /// Capable of handling server state change information.
        /// Its a hint to the server to include the state change information in OK_Packet.
        ///
        /// ### Server
        /// Can set SERVER_SESSION_STATE_CHANGED in the SERVER_STATUS_flags_enum and send
        /// Session State Information in a OK_Packet.
        ///
        /// ### Client
        /// Expects the server to send Session State Information in a OK_Packet.
        const CLIENT_SESSION_TRACK                  = 0x0080_0000;

        /// Client no longer needs EOF_Packet and will use OK_Packet instead.
        ///
        /// ### Server
        /// Can send OK after a Text Resultset.
        ///
        /// ### Client
        /// Expects an OK_Packet (instead of EOF_Packet) after the resultset
        /// rows of a Text Resultset.
        ///
        /// ### Background
        /// To support CLIENT_SESSION_TRACK, additional information must be sent after all
        /// successful commands. Although the OK_Packet is extensible, the EOF_Packet is
        /// not due to the overlap of its bytes with the content of the Text Resultset Row.
        ///
        /// Therefore, the EOF_Packet in the Text Resultset is replaced with an OK_Packet.
        /// EOF_Packet is deprecated as of MySQL 5.7.5.
        const CLIENT_DEPRECATE_EOF                  = 0x0100_0000;

        /// Client or server supports progress reports within error packet.
        const CLIENT_PROGRESS_OBSOLETE              = 0x2000_0000;

        /// Verify server certificate. Client only flag.
        ///
        /// Deprecated in favor of –ssl-mode.
        const CLIENT_SSL_VERIFY_SERVER_CERT         = 0x4000_0000;

        /// Don't reset the options after an unsuccessful connect. Client only flag.
        const CLIENT_REMEMBER_OPTIONS               = 0x8000_0000;
    }
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
/// MySQL connection phase.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/connection-phase.html">Connection Phase</a>
///
#[derive(Debug, PartialEq)]
pub enum MySQLConnectionPhase {
    InitialHandshake,
    AuthPhaseFastPath,
    AuthenticationMethodMismatch,
}

///
/// Column types for MySQL.
///
/// @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type">Column Type</a>
///
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
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
    MysqlTypeJson = 0xf5,
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

impl From<u8> for MySQLColumnType {
    fn from(x: u8) -> MySQLColumnType {
        match x {
            0x00_u8 => MySQLColumnType::MysqlTypeDecimal,
            0x01u8 => MySQLColumnType::MysqlTypeTiny,
            0x02u8 => MySQLColumnType::MysqlTypeShort,
            0x03u8 => MySQLColumnType::MysqlTypeLong,
            0x04u8 => MySQLColumnType::MysqlTypeFloat,
            0x05u8 => MySQLColumnType::MysqlTypeDouble,
            0x06u8 => MySQLColumnType::MysqlTypeNull,
            0x07u8 => MySQLColumnType::MysqlTypeTimestamp,
            0x08u8 => MySQLColumnType::MysqlTypeLonglong,
            0x09u8 => MySQLColumnType::MysqlTypeInt24,
            0x0au8 => MySQLColumnType::MysqlTypeDate,
            0x0bu8 => MySQLColumnType::MysqlTypeTime,
            0x0cu8 => MySQLColumnType::MysqlTypeDatetime,
            0x0du8 => MySQLColumnType::MysqlTypeYear,
            0x0eu8 => MySQLColumnType::MysqlTypeNewDate,
            0x0fu8 => MySQLColumnType::MysqlTypeVarchar,
            0x10u8 => MySQLColumnType::MysqlTypeBit,
            0x11u8 => MySQLColumnType::MysqlTypeTimestamp2,
            0x12u8 => MySQLColumnType::MysqlTypeDatetime2,
            0x13u8 => MySQLColumnType::MysqlTypeTime2,
            0xf5u8 => MySQLColumnType::MysqlTypeJson,
            0xf6u8 => MySQLColumnType::MysqlTypeNewDecimal,
            0xf7u8 => MySQLColumnType::MysqlTypeEnum,
            0xf8u8 => MySQLColumnType::MysqlTypeSet,
            0xf9u8 => MySQLColumnType::MysqlTypeTinyBlob,
            0xfau8 => MySQLColumnType::MysqlTypeMediumBlob,
            0xfbu8 => MySQLColumnType::MysqlTypeLongBlob,
            0xfcu8 => MySQLColumnType::MysqlTypeBlob,
            0xfdu8 => MySQLColumnType::MysqlTypeVarString,
            0xfeu8 => MySQLColumnType::MysqlTypeString,
            0xffu8 => MySQLColumnType::MysqlTypeGeometry,
            _ => panic!("Unknown column type {}", x),
        }
    }
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

bitflags! {
    /// MySql column flags
    pub struct MySQLColumnFlags: u16 {
        /// Field can't be NULL.
        const NOT_NULL_FLAG         = 1u16;

        /// Field is part of a primary key.
        const PRI_KEY_FLAG          = 2u16;

        /// Field is part of a unique key.
        const UNIQUE_KEY_FLAG       = 4u16;

        /// Field is part of a key.
        const MULTIPLE_KEY_FLAG     = 8u16;

        /// Field is a blob.
        const BLOB_FLAG             = 16u16;

        /// Field is unsigned.
        const UNSIGNED_FLAG         = 32u16;

        /// Field is zerofill.
        const ZEROFILL_FLAG         = 64u16;

        /// Field is binary.
        const BINARY_FLAG           = 128u16;

        /// Field is an enum.
        const ENUM_FLAG             = 256u16;

        /// Field is a autoincrement field.
        const AUTO_INCREMENT_FLAG   = 512u16;

        /// Field is a timestamp.
        const TIMESTAMP_FLAG        = 1024u16;

        /// Field is a set.
        const SET_FLAG              = 2048u16;

        /// Field doesn't have default value.
        const NO_DEFAULT_VALUE_FLAG = 4096u16;

        /// Field is set to NOW on UPDATE.
        const ON_UPDATE_NOW_FLAG    = 8192u16;

        /// Intern; Part of some key.
        const PART_KEY_FLAG         = 16384u16;

        /// Field is num (for clients).
        const NUM_FLAG              = 32768u16;
    }
}