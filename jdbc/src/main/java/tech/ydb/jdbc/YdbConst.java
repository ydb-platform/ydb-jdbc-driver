package tech.ydb.jdbc;

public final class YdbConst {

    // SQL types
    public static final int UNKNOWN_SQL_TYPE = Integer.MIN_VALUE;

    public static final int SQL_KIND_PRIMITIVE = 10000;
    public static final int SQL_KIND_DECIMAL = 1 << 14; // 16384

    // Built-in limits
    public static final int MAX_PRIMARY_KEY_SIZE = 1024 * 1024; // 1 MiB per index
    public static final int MAX_COLUMN_SIZE = 4 * 1024 * 1024; // max 4 MiB per value
    public static final int MAX_ROW_SIZE = 4 * 1024 * 1024; // max 8 MiB per row
    public static final int MAX_COLUMN_NAME_LENGTH = 255;
    public static final int MAX_COLUMNS_IN_PRIMARY_KEY = 20;
    public static final int MAX_COLUMNS = 200;
    public static final int MAX_CONNECTIONS = 1000;
    public static final int MAX_ELEMENT_NAME_LENGTH = 255;
    public static final int MAX_STATEMENT_LENGTH = 10 * 1024; // max query size


    // Messages
    public static final String DRIVER_IS_ALREADY_REGISTERED = "Driver is already registered. "
            + "It can only be registered once.";

    public static final String DRIVER_IS_NOT_REGISTERED = "Driver is not registered "
            + "(or it has not been registered using YdbDriver.register() method)";

    public static final String MISSING_DRIVER_OPTION = "Missing value for option ";
    public static final String INVALID_DRIVER_OPTION_VALUE = "Cannot process value %s for option %s: %s";

    public static final String PREPARED_CALLS_UNSUPPORTED = "Prepared calls are not supported";
    public static final String ARRAYS_UNSUPPORTED = "Arrays are not supported";
    public static final String STRUCTS_UNSUPPORTED = "Structs are not supported";
    public static final String BLOB_UNSUPPORTED = "Blobs are not supported";
    public static final String NCLOB_UNSUPPORTED = "NClobs are not supported";
    public static final String CLOB_UNSUPPORTED = "Clobs are not supported";
    public static final String SQLXML_UNSUPPORTED = "SQLXMLs are not supported";
    public static final String SAVEPOINTS_UNSUPPORTED = "Savepoints are not supported";
    public static final String AUTO_GENERATED_KEYS_UNSUPPORTED = "Auto-generated keys are not supported";
    public static final String CURSOR_UPDATING_UNSUPPORTED = "Cursor updates are not supported";
    public static final String ROWID_UNSUPPORTED = "RowIds are not supported";
    public static final String NAMED_CURSORS_UNSUPPORTED = "Named cursors are not supported";
    public static final String REF_UNSUPPORTED = "Refs are not supported";
    public static final String ASCII_STREAM_UNSUPPORTED = "AsciiStreams are not supported";
    public static final String PARAMETERIZED_SCHEME_QUERIES_UNSUPPORTED = "TableService doesn't support parameterized"
            + " scheme queries.";

    public static final String FORWARD_ONLY_MODE = "ResultSet in TYPE_FORWARD_ONLY mode";
    public static final String CUSTOM_SQL_UNSUPPORTED = "PreparedStatement cannot execute custom SQL";
    public static final String ABORT_UNSUPPORTED = "Abort operation is not supported yet";
    public static final String SET_NETWORK_TIMEOUT_UNSUPPORTED = "Set network timeout is not supported yet";
    public static final String OBJECT_TYPED_UNSUPPORTED = "Object with type conversion is not supported yet";
    public static final String QUERY_EXPECT_RESULT_SET = "Query must return ResultSet";
    public static final String QUERY_EXPECT_UPDATE = "Query must not return ResultSet";
    public static final String UNABLE_TO_SET_NULL_OBJECT = "Unable to set null object, type is required";

    public static final String RESULT_SET_MODE_UNSUPPORTED = "ResultSet mode is not supported: ";
    public static final String RESULT_SET_UNAVAILABLE = "ResultSet is not available at index: ";
    public static final String RESULT_SET_IS_CLOSED = "ResultSet is closed";
    public static final String RESULT_IS_TRUNCATED = "Result #%s was truncated to %s rows";
    public static final String RESULT_WAS_INTERRUPTED = "ResultSet reading was interrupted";
    public static final String RESULT_IS_NOT_SCROLLABLE =
            "Requested scrollable ResutlSet, but this ResultSet is FORWARD_ONLY.";
    public static final String RESULT_SET_TYPE_UNSUPPORTED =
            "resultSetType must be ResultSet.TYPE_FORWARD_ONLY or ResultSet.TYPE_SCROLL_INSENSITIVE";
    public static final String RESULT_SET_CONCURRENCY_UNSUPPORTED =
            "resultSetConcurrency must be ResultSet.CONCUR_READ_ONLY";
    public static final String RESULT_SET_HOLDABILITY_UNSUPPORTED =
            "resultSetHoldability must be ResultSet.HOLD_CURSORS_OVER_COMMIT";


    public static final String INVALID_FETCH_DIRECTION = "Fetch direction %s cannot be used when result set type is %s";
    public static final String COLUMN_NOT_FOUND = "Column not found: ";
    public static final String COLUMN_NUMBER_NOT_FOUND = "Column is out of range: ";
    public static final String PARAMETER_NUMBER_NOT_FOUND = "Parameter is out of range: ";
    public static final String PARAMETER_NOT_FOUND = "Parameter not found: ";
    public static final String PARAMETER_TYPE_UNKNOWN = "Unable to convert sqlType %s to YDB type for parameter: %s";
    public static final String INVALID_ROW = "Current row index is out of bounds: ";
    public static final String BULKS_UNSUPPORTED = "BULK mode is available only for prepared statement with one UPSERT";
    public static final String INVALID_BATCH_COLUMN = "Cannot prepared batch request: cannot find a column";
    public static final String BULK_DESCRIBE_ERROR = "Cannot parse BULK upsert: ";
    public static final String BULK_NOT_SUPPORT_RETURNING = "BULK query doesn't support RETURNING";
    public static final String METADATA_RS_UNSUPPORTED_IN_PS = "ResultSet metadata is not supported " +
            "in prepared statements";
    public static final String CANNOT_UNWRAP_TO = "Cannot unwrap to ";
    public static final String READONLY_INSIDE_TRANSACTION = "Cannot change read-only attribute inside a transaction";
    public static final String CHANGE_ISOLATION_INSIDE_TX = "Cannot change transaction isolation inside a transaction";
    public static final String UNSUPPORTED_TRANSACTION_LEVEL = "Unsupported transaction level: ";
    public static final String CLOSED_CONNECTION = "Connection is closed";
    public static final String DB_QUERY_DEADLINE_EXCEEDED = "DB query deadline exceeded: ";
    public static final String DB_QUERY_CANCELLED = "DB query cancelled: ";
    public static final String DATABASE_UNAVAILABLE = "Database is unavailable: ";
    public static final String CANNOT_LOAD_DATA_FROM_IS = "Unable to load data from input stream: ";
    public static final String CANNOT_LOAD_DATA_FROM_READER = "Unable to load data from reader: ";
    public static final String STATEMENT_IS_NOT_A_BATCH = "Statement cannot be executed as batch statement: ";
    public static final String UNABLE_PREPARE_STATEMENT = "Cannot prepare statement: ";
    public static final String MULTI_TYPES_IN_ONE_QUERY = "Query cannot contain expressions with different types: ";
    public static final String SCAN_QUERY_INSIDE_TRANSACTION = "Scan query cannot be executed inside active "
            + "transaction. This behavior may be changed by property scanQueryTxMode";
    public static final String SCHEME_QUERY_INSIDE_TRANSACTION = "Scheme query cannot be executed inside active "
            + "transaction. This behavior may be changed by property schemeQueryTxMode";
    public static final String BULK_QUERY_INSIDE_TRANSACTION = "Bulk upsert query cannot be executed inside active "
            + "transaction. This behavior may be changed by property bulkUpsertQueryTxMode";

    // Cast errors

    // "Cannot cast" is used in tests for checking errors
    public static final String UNABLE_TO_CAST = "Cannot cast [%s] to [%s]";
    public static final String UNABLE_TO_CONVERT = "Cannot cast [%s] with value [%s] to [%s]";
    public static final String UNABLE_TO_CONVERT_AS_URL = "Cannot cast as URL: ";
    public static final String UNABLE_TO_CAST_TO_CLASS = "Cannot cast [%s] to class [%s]";
    public static final String UNABLE_TO_CAST_TO_DECIMAL = "Cannot cast to decimal type %s: [%s] is %s";

    public static final String MISSING_VALUE_FOR_PARAMETER = "Missing value for parameter: ";
    public static final String MISSING_REQUIRED_VALUE = "Missing required value for parameter: ";
    public static final String INVALID_PARAMETER_TYPE = "Cannot cast parameter [%s] from [%s] to [%s]";

    // Custom transaction levels
    // See details in https://ydb.tech/docs/en/concepts/transactions#modes

    /**
     * The most recent consistent state of the database. Read only.
     */
    public static final int ONLINE_CONSISTENT_READ_ONLY = 16;
    /**
     * The most recent inconsistent state of the database. Read only.
     * A phantom read may occurs when, in the course of a transaction, some new rows are added
     * by another transaction to the records being read. This is the weakest level.
     */
    public static final int ONLINE_INCONSISTENT_READ_ONLY = ONLINE_CONSISTENT_READ_ONLY + 1;

    /**
     * An <em>almost</em> recent consistent state of the database. Read only.
     * This level is faster then {@code ONLINE_CONSISTENT_READ_ONLY}, but may return stale data.
     */
    public static final int STALE_CONSISTENT_READ_ONLY = 32;

    // Processing queries
    public static final String EXPLAIN_COLUMN_AST = "AST";
    public static final String EXPLAIN_COLUMN_PLAN = "PLAN";

    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_YDB_PREFIX = JDBC_PREFIX + "ydb:";

    // All indexed parameters will have this 'p' as a prefix, setInteger(1, "test") -> setInteger("p1", "test")
    public static final String INDEXED_PARAMETER_PREFIX = "p";
    public static final String VARIABLE_PARAMETER_PREFIX = "$";
    public static final String AUTO_GENERATED_PARAMETER_PREFIX = VARIABLE_PARAMETER_PREFIX + "jp";

    private YdbConst() {
        //
    }
}
