package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbDatabaseMetaData;
import tech.ydb.jdbc.YdbDriverInfo;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.common.YdbFunctions;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;


public class YdbDatabaseMetaDataImpl implements YdbDatabaseMetaData {
    private static final Logger LOGGER = Logger.getLogger(YdbDatabaseMetaDataImpl.class.getName());

    static final String TABLE = "TABLE";
    static final String SYSTEM_TABLE = "SYSTEM TABLE";

    private final YdbConnectionImpl connection;
    private final YdbTypes types;
    private final YdbExecutor executor;

    public YdbDatabaseMetaDataImpl(YdbConnectionImpl connection) {
        this.connection = Objects.requireNonNull(connection);
        this.types = connection.getYdbTypes();
        this.executor = new YdbExecutor(LOGGER);
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return connection.getCtx().getUrl();
    }

    @Override
    public String getUserName() {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return "YDB";
    }

    @Override
    public String getDatabaseProductVersion() {
        return "unspecified"; // TODO: don't know how to get YDB version
    }

    @Override
    public String getDriverName() {
        return YdbDriverInfo.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return YdbDriverInfo.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return YdbDriverInfo.DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return YdbDriverInfo.DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() {
        return QUOTE_IDENTIFIER;
    }

    @Override
    public String getSQLKeywords() {
        return ""; // TODO: unknown?
    }

    @Override
    public String getNumericFunctions() {
        return YdbFunctions.NUMERIC_FUNCTIONS;
    }

    @Override
    public String getStringFunctions() {
        return YdbFunctions.STRING_FUNCTIONS;
    }

    @Override
    public String getSystemFunctions() {
        return YdbFunctions.SYSTEM_FUNCTIONS;
    }

    @Override
    public String getTimeDateFunctions() {
        return YdbFunctions.DATETIME_FUNCTIONS;
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false; // Probably not
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false; // Probably not
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true; // multiple transactions in different connections
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false; // not yet
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true; // think so
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false; // no
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false; // no
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false; // no
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false; // no
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false; // no
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false; // no
    }

    @Override
    public boolean supportsOuterJoins() {
        return true; // yes
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true; // yes
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true; // yes
    }

    @Override
    public String getSchemaTerm() {
        return "Database";
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return "Path";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return "/";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false; // No
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false; // No
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false; // Pessimistic locks are not supported
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false; // not supported
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return false; // only All supported
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return true; // yes
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return true;  // yes
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true; // yes
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true; // yes
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return YdbConst.MAX_COLUMN_SIZE;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return YdbConst.MAX_COLUMN_SIZE;
    }

    @Override
    public int getMaxColumnNameLength() {
        return YdbConst.MAX_COLUMN_NAME_LENGTH;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return YdbConst.MAX_COLUMNS;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return YdbConst.MAX_COLUMNS_IN_PRIMARY_KEY;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return YdbConst.MAX_COLUMNS;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return YdbConst.MAX_COLUMNS;
    }

    @Override
    public int getMaxColumnsInTable() {
        return YdbConst.MAX_COLUMNS;
    }

    @Override
    public int getMaxConnections() {
        return YdbConst.MAX_CONNECTIONS;
    }

    @Override
    public int getMaxCursorNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getMaxIndexLength() {
        return YdbConst.MAX_PRIMARY_KEY_SIZE;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getMaxRowSize() {
        return YdbConst.MAX_ROW_SIZE;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getMaxStatementLength() {
        return YdbConst.MAX_STATEMENT_LENGTH;
    }

    @Override
    public int getMaxStatements() {
        return 0; // no limit for statements (statement is opened only in memory)
    }

    @Override
    public int getMaxTableNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0; // Limit is unknown
    }

    @Override
    public int getMaxUserNameLength() {
        return YdbConst.MAX_ELEMENT_NAME_LENGTH;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE;
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        switch (level) {
            case YdbConst.TRANSACTION_SERIALIZABLE_READ_WRITE:
            case YdbConst.ONLINE_CONSISTENT_READ_ONLY:
            case YdbConst.ONLINE_INCONSISTENT_READ_ONLY:
            case YdbConst.STALE_CONSISTENT_READ_ONLY:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return true; // basically yes, but DDL executed outsize of a transaction
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return true; // transaction will be prepared anyway
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false; // DDL outside of transactions
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return true; // DDL outside of transactions
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        return fromEmptyResultSet(); // Procedures are not supported
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        return fromEmptyResultSet(); // Procedures are not supported
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        LOGGER.log(Level.FINE,
                "getTables, catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], types={3}",
                new Object[]{catalog, schemaPattern, tableNamePattern, types == null ? "<null>" : Arrays.asList(types)}
        );
        if (!isMatchedCatalog(catalog)) {
            return fromEmptyResultSet();
        }
        if (!isMatchedSchema(schemaPattern)) {
            return fromEmptyResultSet();
        }

        boolean matchTables;
        boolean matchSystemTables;

        if (types == null) {
            matchTables = true;
            matchSystemTables = true;
        } else {
            Set<String> typesSet = new HashSet<>(Arrays.asList(types));
            matchTables = typesSet.contains(TABLE);
            matchSystemTables = typesSet.contains(SYSTEM_TABLE);
        }

        if (!matchTables && !matchSystemTables) {
            return fromEmptyResultSet();
        }

        List<Map<String, Object>> rows = listTables(tableNamePattern).stream()
                .map(tableName -> MappingResultSets.stableMap(
                        "TABLE_CAT", null,
                        "TABLE_SCHEM", null,
                        "TABLE_NAME", tableName,
                        "TABLE_TYPE", getTableType(tableName),
                        "REMARKS", null,
                        "TYPE_CAT", null,
                        "TYPE_SCHEM", null,
                        "TYPE_NAME", null,
                        "SELF_REFERENCING_COL_NAME", null,
                        "REF_GENERATION", null
                ))
                .filter(map -> (matchTables || !TABLE.equals(map.get("TABLE_TYPE"))) &&
                        (matchSystemTables || !SYSTEM_TABLE.equals(map.get("TABLE_TYPE"))))
                .sorted(Comparator
                        .comparing((Map<String, Object> m) -> (String) m.get("TABLE_TYPE"))
                        .thenComparing(m -> (String) m.get("TABLE_NAME")))
                .collect(Collectors.toList());

        return fromRows(rows);
    }

    private String getTableType(String tableName) {
        if (tableName.startsWith(".sys/") || tableName.startsWith(".sys_health/")) {
            return SYSTEM_TABLE;
        }
        return TABLE;
    }

    @Override
    public ResultSet getSchemas() {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() {
        return fromEmptyResultSet(); // Does not support catalogs, all table names has full catalog prefix
    }

    @Override
    public ResultSet getTableTypes() {
        return fromRows(Arrays.asList(
                Collections.singletonMap("TABLE_TYPE", TABLE),
                Collections.singletonMap("TABLE_TYPE", SYSTEM_TABLE)
        ));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        LOGGER.log(Level.FINE,
                "getColumns, catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], columnNamePattern=[{3}]",
                new Object[]{catalog, schemaPattern, tableNamePattern, columnNamePattern}
        );

        if (!isMatchedCatalog(catalog)) {
            return fromEmptyResultSet();
        }

        if (!isMatchedSchema(schemaPattern)) {
            return fromEmptyResultSet();
        }

        Predicate<String> columnFilter = equalsFilter(columnNamePattern);
        return fromTables(tableNamePattern,
                (tableName, tableDesc, rows) -> {
                    short index = 0;
                    for (TableColumn column : tableDesc.getColumns()) {
                        index++;
                        if (!columnFilter.test(column.getName())) {
                            continue;
                        }
                        Type type = column.getType();

                        int nullable;
                        if (type.getKind() == Type.Kind.OPTIONAL) {
                            nullable = columnNullable;
                            type = type.unwrapOptional();
                        } else {
                            nullable = columnNoNulls;
                        }

                        rows.add(MappingResultSets.stableMap(
                                "TABLE_CAT", null,
                                "TABLE_SCHEM", null,
                                "TABLE_NAME", tableName,
                                "COLUMN_NAME", column.getName(),
                                "DATA_TYPE", types.toSqlType(type),
                                "TYPE_NAME", type.toString(),
                                "COLUMN_SIZE", types.getSqlPrecision(type),
                                "BUFFER_LENGTH", 0,
                                "DECIMAL_DIGITS", (short)(type.getKind() == Type.Kind.DECIMAL ?
                                        YdbConst.SQL_DECIMAL_DEFAULT_PRECISION :
                                        0),
                                "NUM_PREC_RADIX", 10,
                                "NULLABLE", nullable,
                                "REMARKS", null,
                                "COLUMN_DEF", null, // no default values
                                "SQL_DATA_TYPE", 0,
                                "SQL_DATETIME_SUB", 0,
                                "CHAR_OCTET_LENGTH", 0, // unsupported yet
                                "ORDINAL_POSITION", index,
                                "IS_NULLABLE", "YES",
                                "SCOPE_CATALOG", null,
                                "SCOPE_SCHEMA", null,
                                "SCOPE_TABLE", null,
                                "SOURCE_DATA_TYPE", (short) 0,
                                "IS_AUTOINCREMENT", "NO", // no auto increments
                                "IS_GENERATEDCOLUMN", "NO" // no generated columns
                        ));
                    }
                },
                Comparator
                        .comparing((Map<String, Object> m) -> (String) m.get("TABLE_NAME"))
                        .thenComparingInt(m -> (Short) m.get("ORDINAL_POSITION")));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        return fromEmptyResultSet(); // No column-based privileges supported
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        return fromEmptyResultSet(); // Unable to collect privileges
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        LOGGER.log(Level.FINE,
                "getBestRowIdentifier, catalog=[{0}], schema=[{1}], table=[{2}], scope=[{3}], nullable=[{4}]",
                new Object[]{catalog, schema, table, scope, nullable}
        );

        if (!isMatchedCatalog(catalog)) {
            return fromEmptyResultSet();
        }
        if (!isMatchedSchema(schema)) {
            return fromEmptyResultSet();
        }
        if (isMatchedAny(table)) {
            return fromEmptyResultSet(); // must be table name
        }
        if (!nullable) {
            return fromEmptyResultSet();
        }

        // Only primary keys could be used as row identifiers
        return fromTables(table,
                (tableName, tableDesc, rows) -> {
                    Map<String, TableColumn> columnMap = tableDesc.getColumns().stream()
                            .collect(Collectors.toMap(TableColumn::getName, Function.identity()));
                    for (String key : tableDesc.getPrimaryKeys()) {

                        TableColumn column = columnMap.get(key);
                        Type type = column.getType();
                        if (type.getKind() == Type.Kind.OPTIONAL) {
                            type = type.unwrapOptional();
                        }

                        rows.add(MappingResultSets.stableMap(
                                "SCOPE", (short)scope,
                                "COLUMN_NAME", key,
                                "DATA_TYPE", types.toSqlType(type),
                                "TYPE_NAME", type.toString(),
                                "COLUMN_SIZE", 0,
                                "BUFFER_LENGTH", 0,
                                "DECIMAL_DIGITS", (short) 0, // unknown
                                "PSEUDO_COLUMN", bestRowNotPseudo));
                    }

                },
                Comparator.comparing(m -> (Short) m.get("SCOPE")));
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        return fromEmptyResultSet(); // Version columns are not supported
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        LOGGER.log(Level.FINE, "getPrimaryKeys, catalog=[{0}], schema=[{1}], table=[{2}]",  new Object[]{
            catalog, schema, table
        });

        if (!isMatchedCatalog(catalog)) {
            return fromEmptyResultSet();
        }

        if (!isMatchedSchema(schema)) {
            return fromEmptyResultSet();
        }

        if (isMatchedAny(table)) {
            return fromEmptyResultSet();
        }

        return fromTables(table,
                (tableName, tableDesc, rows) -> {
                    short index = 0;
                    for (String key : tableDesc.getPrimaryKeys()) {
                        index++;
                        rows.add(MappingResultSets.stableMap(
                                "TABLE_CAT", null,
                                "TABLE_SCHEM", null,
                                "TABLE_NAME", tableName,
                                "COLUMN_NAME", key,
                                "KEY_SEQ", index,
                                "PK_NAME", null));
                    }

                },
                Comparator.comparing(m -> (String) m.get("COLUMN_NAME")));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        return fromEmptyResultSet(); // Foreign keys are not supported
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        return fromEmptyResultSet();  // Foreign keys are not supported
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        return fromEmptyResultSet();  // Foreign keys are not supported
    }

    @Override
    public ResultSet getTypeInfo() {
        List<Map<String, Object>> rows = types.getAllDatabaseTypes().stream()
                .map(type -> {
                    String literal = getLiteral(type);
                    int scale = type.getKind() == Type.Kind.DECIMAL ? YdbConst.SQL_DECIMAL_DEFAULT_SCALE : 0;
                    return MappingResultSets.stableMap(
                            "TYPE_NAME", type.toString(),
                            "DATA_TYPE", types.toSqlType(type),
                            "PRECISION", types.getSqlPrecision(type),
                            "LITERAL_PREFIX", literal,
                            "LITERAL_SUFFIX", literal,
                            "CREATE_PARAMS", null,
                            "NULLABLE", typeNullable,
                            "CASE_SENSITIVE", true,
                            "SEARCHABLE", getSearchable(type),
                            "UNSIGNED_ATTRIBUTE", getUnsigned(type),
                            "FIXED_PREC_SCALE", type.getKind() == Type.Kind.DECIMAL,
                            "AUTO_INCREMENT", false, // no auto-increments
                            "LOCAL_TYPE_NAME", null,
                            "MINIMUM_SCALE", scale,
                            "MAXIMUM_SCALE", scale,
                            "SQL_DATA_TYPE", 0,
                            "SQL_DATETIME_SUB", 0,
                            "NUM_PREC_RADIX", 10
                    );
                })
                .sorted(Comparator.comparing((Map<String, Object> m) -> (Integer) m.get("DATA_TYPE")))
                .collect(Collectors.toList());


        return fromRows(rows);
    }

    private short getSearchable(Type type) {
        if (type.getKind() == Type.Kind.PRIMITIVE) {
            switch ((PrimitiveType) type) {
                case Json:
                case JsonDocument:
                case Yson:
                    return typePredNone;
                case Bytes:
                case Text:
                    return typeSearchable;
                default:
                    return typePredBasic;
            }
        } else {
            return typePredBasic;
        }
    }

    private boolean getUnsigned(Type type) {
        if (type.getKind() == Type.Kind.PRIMITIVE) {
            switch ((PrimitiveType) type) {
                case Uint8:
                case Uint16:
                case Uint32:
                case Uint64:
                    return true;
                default:
                    //
            }
        }
        return false;
    }

    @Nullable
    private String getLiteral(Type type) {
        if (type.getKind() == Type.Kind.PRIMITIVE) {
            switch ((PrimitiveType) type) {
                case Bytes:
                case Text:
                case Json:
                case JsonDocument:
                case Yson:
                    return "'";
            }
        }
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        LOGGER.log(Level.FINE,
                "getIndexInfo, catalog=[{0}], schema=[{1}], table=[{2}], unique=[{3}], approximate=[{4}]",
                new Object[]{catalog, schema, table, unique, approximate}
        );

        if (!isMatchedCatalog(catalog)) {
            return fromEmptyResultSet();
        }

        if (!isMatchedSchema(schema)) { // not exactly the same schema
            return fromEmptyResultSet();
        }

        if (isMatchedAny(table)) {
            return fromEmptyResultSet(); // must be table name
        }

        if (unique) {
            return fromEmptyResultSet();
        }

        return fromTables(table,
                (tableName, tableDesc, rows) -> {
                    for (TableIndex tableIndex : tableDesc.getIndexes()) {
                        short index = 0;
                        for (String column : tableIndex.getColumns()) {
                            index++;
                            rows.add(MappingResultSets.stableMap(
                                    "TABLE_CAT", null,
                                    "TABLE_SCHEM", null,
                                    "TABLE_NAME", tableName,
                                    "NON_UNIQUE", true,
                                    "INDEX_QUALIFIER", null,
                                    "INDEX_NAME", tableIndex.getName(),
                                    "TYPE", tableIndexHashed, // just an index?
                                    "ORDINAL_POSITION", index,
                                    "COLUMN_NAME", column,
                                    "ASC_OR_DESC", null, // unknown sort sequence?
                                    "CARDINALITY", 0,
                                    "PAGES", 0,
                                    "FILTER_CONDITION", null));
                        }
                    }
                },
                Comparator
                        .comparing((Map<String, Object> m) -> (Boolean) m.get("NON_UNIQUE"))
                        .thenComparing(m -> (Short) m.get("TYPE"))
                        .thenComparing(m -> (String) m.get("INDEX_NAME"))
                        .thenComparing(m -> (Short) m.get("ORDINAL_POSITION")));
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return (type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE) &&
                concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false; // cursor updates not supported
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true; // yes, but with special form of a batch
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        return fromEmptyResultSet(); // UDTs are not supported
    }

    @Override
    public YdbConnection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false; // savepoints are not supported
    }

    @Override
    public boolean supportsNamedParameters() {
        return false; // callables are not supported
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false; // callables are not supported
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false; // generated keys are not supported
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        return fromEmptyResultSet();  // UDTs are not supported
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        return fromEmptyResultSet(); // Super-tables are not supported
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        return fromEmptyResultSet(); // UDTss are not supported
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0; // unknown
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0; // unknown
    }

    @Override
    public int getJDBCMajorVersion() {
        return YdbDriverInfo.JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() {
        return YdbDriverInfo.JDBC_MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false; // ?
    }

    @Override
    public boolean supportsStatementPooling() {
        return true; // looks so
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        return fromEmptyResultSet();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false; // Calls are not supported
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false; // All result sets still opened
    }

    @Override
    public ResultSet getClientInfoProperties() {
        return fromEmptyResultSet(); // No client info getOperationProperties?
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        return fromEmptyResultSet();  // Custom functions are not supported
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        return fromEmptyResultSet(); // Custom functions are not supported
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        return fromEmptyResultSet(); // Pseudo columns are not supported
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false; // No generated keys
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(YdbConst.CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    private Map<String, TableDescription> collectTableDescriptions(Session session, Collection<String> tables) throws SQLException {
        DescribeTableSettings settings = connection.withDefaultTimeout(new DescribeTableSettings());
        String databaseWithSuffix = withSuffix(connection.getCtx().getDatabase());

        Map<String, TableDescription> target = new LinkedHashMap<>(tables.size());

        for (String table: tables) {
            target.put(table, executor.call("Get table description for " + table,
                    () -> session.describeTable(databaseWithSuffix + table, settings)
            ));
        }

        return target;
    }

    private Collection<String> listTables(String tableNamePattern) throws SQLException {
        Predicate<String> filter = equalsFilter(tableNamePattern);

        Collection<String> allTables = listTables(filter);
        LOGGER.log(Level.FINE, "Loaded {0} tables...", allTables.size());

        return allTables;
    }

    private Collection<String> listTables(Predicate<String> filter) throws SQLException {
        String databaseWithSuffix = withSuffix(connection.getCtx().getDatabase());
        return tables(databaseWithSuffix, databaseWithSuffix, filter);
    }

    private List<String> tables(String databasePrefix, String path, Predicate<String> filter) throws SQLException {
        SchemeClient client = connection.getCtx().getSchemeClient();
        ListDirectoryResult result = executor.call("List tables from " + path, () -> client.listDirectory(path));

        List<String> tables = new ArrayList<>();
        String pathPrefix = withSuffix(path);

        for (SchemeOperationProtos.Entry entry : result.getChildren()) {
            String tableName = entry.getName();
            String fullPath = pathPrefix + tableName;
            String tablePath = fullPath.substring(databasePrefix.length());
            switch (entry.getType()) {
                case TABLE:
                case COLUMN_TABLE:
                    if (filter.test(tablePath)) {
                        tables.add(tablePath);
                    }
                    break;
                case DIRECTORY:
                    tables.addAll(tables(databasePrefix, fullPath, filter));
                    break;
                default:
                    // skip
            }
        }
        return tables;
    }

    private ResultSet fromTables(String tableNamePattern,
                                 TableCollector tableCollector,
                                 Comparator<Map<String, Object>> comparator) throws SQLException {
        Collection<String> tables = listTables(tableNamePattern);
        if (tables.isEmpty()) {
            return fromEmptyResultSet();
        }

        try (Session session = executor.createSession(connection.getCtx())) {
            Map<String, TableDescription> tableMap = collectTableDescriptions(session, tables);

            List<Map<String, Object>> rows = new ArrayList<>(tableMap.size() * 16);
            for (Map.Entry<String, TableDescription> entry : tableMap.entrySet()) {
                tableCollector.collect(entry.getKey(), entry.getValue(), rows);
            }
            rows.sort(comparator);
            return fromRows(rows);
        }
    }

    private ResultSet fromEmptyResultSet() {
        YdbStatementImpl statement = new YdbStatementImpl(connection, ResultSet.TYPE_SCROLL_INSENSITIVE);
        ResultSetReader reader = MappingResultSets.emptyReader();
        return new YdbResultSetImpl(statement, reader);
    }

    private ResultSet fromRows(List<Map<String, Object>> rows) {
        YdbStatementImpl statement = new YdbStatementImpl(connection, ResultSet.TYPE_SCROLL_INSENSITIVE);
        ResultSetReader reader = MappingResultSets.readerFromList(rows);
        return new YdbResultSetImpl(statement, reader);
    }

    private boolean isMatchedCatalog(String catalog) {
        return isMatchedAny(catalog);
    }

    private boolean isMatchedSchema(String schema) throws SQLException {
        return isMatchedAny(schema) || Objects.equals(withSuffix(schema), withSuffix(connection.getSchema()));
    }

    private static boolean isMatchedAny(String filter) {
        return Strings.isNullOrEmpty(filter) || filter.equals("%");
    }

    private static Predicate<String> equalsFilter(String name) {
        if (isMatchedAny(name)) {
            return table -> true;
        } else {
            return name::equals;
        }
    }

    static String withSuffix(String prefix) {
        return prefix == null || prefix.endsWith("/") ? prefix : prefix + "/";
    }

    interface TableCollector {
        void collect(String tableName, TableDescription tableDescription, List<Map<String, Object>> rows);
    }
}
