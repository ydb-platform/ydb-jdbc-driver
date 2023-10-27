package tech.ydb.jdbc.impl;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbDatabaseMetaData;
import tech.ydb.jdbc.YdbDriverInfo;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.jdbc.common.YdbFunctions;
import tech.ydb.jdbc.context.YdbExecutor;
import tech.ydb.jdbc.exception.YdbStatusException;
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
        // Procedures are not supported
        return emptyResultSet(MetaDataTables.PROCEDURES);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        // Procedures are not supported
        return emptyResultSet(MetaDataTables.PROCEDURE_COLUMNS);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        LOGGER.log(Level.FINE,
                "getTables, catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], types={3}",
                new Object[]{catalog, schemaPattern, tableNamePattern, types == null ? "<null>" : Arrays.asList(types)}
        );
        if (!isMatchedCatalog(catalog)) {
            return emptyResultSet(MetaDataTables.TABLES);
        }
        if (!isMatchedSchema(schemaPattern)) {
            return emptyResultSet(MetaDataTables.TABLES);
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
            return emptyResultSet(MetaDataTables.TABLES);
        }

        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.TABLES.createResultSet();
        listTables(tableNamePattern).stream()
                .map(TableRecord::new)
                .filter(tr -> (matchTables && !tr.isSystem) || (matchSystemTables && tr.isSystem))
                .sorted()
                .forEach(tr -> {
                    rs.newRow()
                            .withTextValue("TABLE_NAME", tr.name)
                            .withTextValue("TABLE_TYPE", tr.isSystem ? SYSTEM_TABLE : TABLE)
                            .build();
                });

        return resultSet(rs.build());
    }

    private class TableRecord implements Comparable<TableRecord> {
        private final boolean isSystem;
        private final String name;

        public TableRecord(String name) {
            this.name = name;
            this.isSystem = name.startsWith(".sys/") || name.startsWith(".sys_health/");
        }

        @Override
        public int compareTo(TableRecord o) {
            if (isSystem != o.isSystem) {
                return isSystem ? 1 : -1;
            }
            return name.compareTo(o.name);
        }
    }

    @Override
    public ResultSet getSchemas() {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() {
        // Does not support catalogs, all table names has full catalog prefix
        return emptyResultSet(MetaDataTables.CATALOGS);
    }

    @Override
    public ResultSet getTableTypes() {
        ResultSetReader rs = MetaDataTables.TABLE_TYPES.createResultSet()
                .newRow().withTextValue("TABLE_TYPE", TABLE).build()
                .newRow().withTextValue("TABLE_TYPE", SYSTEM_TABLE).build()
                .build();
        return resultSet(rs);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        LOGGER.log(Level.FINE,
                "getColumns, catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], columnNamePattern=[{3}]",
                new Object[]{catalog, schemaPattern, tableNamePattern, columnNamePattern}
        );

        if (!isMatchedCatalog(catalog)) {
            return emptyResultSet(MetaDataTables.COLUMNS);
        }

        if (!isMatchedSchema(schemaPattern)) {
            return emptyResultSet(MetaDataTables.COLUMNS);
        }

        Predicate<String> columnFilter = equalsFilter(columnNamePattern);
        List<String> tableNames = listTables(tableNamePattern);
        Collections.sort(tableNames);

        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.COLUMNS.createResultSet();
        for (String tableName: tableNames) {
            TableDescription tableDescription = describeTable(tableName);
            if (tableDescription == null) {
                continue;
            }

            short index = 0;
            for (TableColumn column : tableDescription.getColumns()) {
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

                int decimalDigits = type.getKind() == Type.Kind.DECIMAL ? YdbConst.SQL_DECIMAL_DEFAULT_PRECISION : 0;

                rs.newRow()
                        .withTextValue("TABLE_NAME", tableName)
                        .withTextValue("COLUMN_NAME", column.getName())
                        .withIntValue("DATA_TYPE", types.toSqlType(type))
                        .withTextValue("TYPE_NAME", type.toString())
                        .withIntValue("COLUMN_SIZE", types.getSqlPrecision(type))
                        .withIntValue("BUFFER_LENGTH", 0)
                        .withIntValue("DECIMAL_DIGITS", decimalDigits)
                        .withIntValue("NUM_PREC_RADIX", 10)
                        .withIntValue("NULLABLE", nullable)
                        .withIntValue("SQL_DATA_TYPE", 0)
                        .withIntValue("SQL_DATETIME_SUB", 0)
                        .withIntValue("CHAR_OCTET_LENGTH", 0) // unsupported yet
                        .withIntValue("ORDINAL_POSITION", index)
                        .withTextValue("IS_NULLABLE", "YES")
                        .withShortValue("SOURCE_DATA_TYPE", (short) 0)
                        .withTextValue("IS_AUTOINCREMENT", "NO") // no auto increments
                        .withTextValue("IS_GENERATEDCOLUMN", "NO") // no generated columns
                        .build();
            }
        }

        return resultSet(rs.build());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        // No column-based privileges supported
        return emptyResultSet(MetaDataTables.COLUMN_PRIVILEGES);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        // Unable to collect privileges
        return emptyResultSet(MetaDataTables.TABLE_PRIVILEGES);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        LOGGER.log(Level.FINE,
                "getBestRowIdentifier, catalog=[{0}], schema=[{1}], table=[{2}], scope=[{3}], nullable=[{4}]",
                new Object[]{catalog, schema, table, scope, nullable}
        );

        if (!isMatchedCatalog(catalog)) {
            return emptyResultSet(MetaDataTables.BEST_ROW_IDENTIFIERS);
        }
        if (!isMatchedSchema(schema)) {
            return emptyResultSet(MetaDataTables.BEST_ROW_IDENTIFIERS);
        }
        if (isMatchedAny(table)) {
            // must be table name
            return emptyResultSet(MetaDataTables.BEST_ROW_IDENTIFIERS);
        }
        if (!nullable) {
            return emptyResultSet(MetaDataTables.BEST_ROW_IDENTIFIERS);
        }

        TableDescription description = describeTable(table);
        if (description == null) {
            return emptyResultSet(MetaDataTables.BEST_ROW_IDENTIFIERS);
        }

        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.BEST_ROW_IDENTIFIERS.createResultSet();

        Map<String, TableColumn> columnMap = description.getColumns().stream()
                .collect(Collectors.toMap(TableColumn::getName, Function.identity()));

        // Only primary keys could be used as row identifiers
        for (String key : description.getPrimaryKeys()) {
            TableColumn column = columnMap.get(key);
            Type type = column.getType();
            if (type.getKind() == Type.Kind.OPTIONAL) {
                type = type.unwrapOptional();
            }

            int decimalDigits = type.getKind() == Type.Kind.DECIMAL ? YdbConst.SQL_DECIMAL_DEFAULT_PRECISION : 0;

            rs.newRow()
                    .withShortValue("SCOPE", (short)scope)
                    .withTextValue("COLUMN_NAME", key)
                    .withIntValue("DATA_TYPE", types.toSqlType(type))
                    .withTextValue("TYPE_NAME", type.toString())
                    .withIntValue("COLUMN_SIZE", 0)
                    .withIntValue("BUFFER_LENGTH", 0)
                    .withShortValue("DECIMAL_DIGITS", (short) decimalDigits)
                    .withShortValue("PSEUDO_COLUMN", (short)bestRowNotPseudo)
                    .build();
        }

        return resultSet(rs.build());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        // Version columns are not supported
        return emptyResultSet(MetaDataTables.VERSION_COLUMNS);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        LOGGER.log(Level.FINE, "getPrimaryKeys, catalog=[{0}], schema=[{1}], table=[{2}]",  new Object[]{
            catalog, schema, table
        });

        if (!isMatchedCatalog(catalog)) {
            return emptyResultSet(MetaDataTables.PRIMARY_KEYS);
        }

        if (!isMatchedSchema(schema)) {
            return emptyResultSet(MetaDataTables.PRIMARY_KEYS);
        }

        if (isMatchedAny(table)) {
            return emptyResultSet(MetaDataTables.PRIMARY_KEYS);
        }

        TableDescription description = describeTable(table);
        if (description == null) {
            return emptyResultSet(MetaDataTables.PRIMARY_KEYS);
        }

        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.PRIMARY_KEYS.createResultSet();
        short index = 0;
        for (String key : description.getPrimaryKeys()) {
            index++;
            rs.newRow()
                    .withTextValue("TABLE_NAME", table)
                    .withTextValue("COLUMN_NAME", key)
                    .withShortValue("KEY_SEQ", index)
                    .build();
        }
        return resultSet(rs.build());
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        // Foreign keys are not supported
        return emptyResultSet(MetaDataTables.IMPORTED_KEYS);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        // Foreign keys are not supported
        return emptyResultSet(MetaDataTables.EXPORTED_KEYS);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        // Foreign keys are not supported
        return emptyResultSet(MetaDataTables.CROSS_REFERENCES);
    }

    @Override
    public ResultSet getTypeInfo() {
        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.TYPE_INFOS.createResultSet();

        for (Type type: types.getAllDatabaseTypes()) {
            String literal = getLiteral(type);
            int scale = type.getKind() == Type.Kind.DECIMAL ? YdbConst.SQL_DECIMAL_DEFAULT_SCALE : 0;
            rs.newRow()
                    .withTextValue("TYPE_NAME", type.toString())
                    .withIntValue("DATA_TYPE", types.toSqlType(type))
                    .withIntValue("PRECISION", types.getSqlPrecision(type))
                    .withTextValue("LITERAL_PREFIX", literal)
                    .withTextValue("LITERAL_SUFFIX", literal)
                    .withShortValue("NULLABLE", (short)typeNullable)
                    .withBoolValue("CASE_SENSITIVE", true)
                    .withShortValue("SEARCHABLE", getSearchable(type))
                    .withBoolValue("UNSIGNED_ATTRIBUTE", getUnsigned(type))
                    .withBoolValue("FIXED_PREC_SCALE", type.getKind() == Type.Kind.DECIMAL)
                    .withBoolValue("AUTO_INCREMENT", false) // no auto-increments
                    .withShortValue("MINIMUM_SCALE", (short)scale)
                    .withShortValue("MAXIMUM_SCALE", (short)scale)
                    .withIntValue("SQL_DATA_TYPE", 0)
                    .withIntValue("SQL_DATETIME_SUB", 0)
                    .withIntValue("NUM_PREC_RADIX", 10)
                    .build();
        }

        return resultSet(rs.build());
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
            return emptyResultSet(MetaDataTables.INDEX_INFOS);
        }

        if (!isMatchedSchema(schema)) { // not exactly the same schema
            return emptyResultSet(MetaDataTables.INDEX_INFOS);
        }

        if (isMatchedAny(table)) {
            return emptyResultSet(MetaDataTables.INDEX_INFOS);
        }

        if (unique) {
            return emptyResultSet(MetaDataTables.INDEX_INFOS);
        }

        TableDescription description = describeTable(table);
        if (description == null) {
            return emptyResultSet(MetaDataTables.INDEX_INFOS);
        }

        FixedResultSetFactory.ResultSetBuilder rs = MetaDataTables.INDEX_INFOS.createResultSet();
        for (TableIndex tableIndex : description.getIndexes()) {
            short index = 0;
            for (String column : tableIndex.getColumns()) {
                index++;
                rs.newRow()
                        .withTextValue("TABLE_NAME", table)
                        .withBoolValue("NON_UNIQUE", true)
                        .withTextValue("INDEX_NAME", tableIndex.getName())
                        .withShortValue("TYPE", tableIndexHashed) // just an index?
                        .withShortValue("ORDINAL_POSITION", index)
                        .withTextValue("COLUMN_NAME", column)
                        .withLongValue("CARDINALITY", 0)
                        .withLongValue("PAGES", 0)
                        .build();
            }
        }
        return resultSet(rs.build());
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
        // UDTs are not supported
        return emptyResultSet(MetaDataTables.UDTS);
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
        // Super-types are not supported
        return emptyResultSet(MetaDataTables.SUPER_TYPES);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        // Super-tables are not supported
        return emptyResultSet(MetaDataTables.SUPER_TABLES);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) {
        // Attributes are not supported
        return emptyResultSet(MetaDataTables.ATTRIBUTES);
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
        return emptyResultSet(MetaDataTables.SCHEMAS);
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
        // No client info getOperationProperties?
        return emptyResultSet(MetaDataTables.CLIENT_INFO_PROPERTIES);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        // Custom functions are not supported
        return emptyResultSet(MetaDataTables.FUNCTIONS);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        // Custom functions are not supported
        return emptyResultSet(MetaDataTables.FUNCTION_COLUMNS);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        // Pseudo columns are not supported
        return emptyResultSet(MetaDataTables.PSEUDO_COLUMNS);
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

    private List<String> listTables(String tableNamePattern) throws SQLException {
        Predicate<String> filter = equalsFilter(tableNamePattern);

        List<String> allTables = listTables(filter);
        LOGGER.log(Level.FINE, "Loaded {0} tables...", allTables.size());

        return allTables;
    }

    private List<String> listTables(Predicate<String> filter) throws SQLException {
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

    private TableDescription describeTable(String table) throws SQLException {
        DescribeTableSettings settings = connection.withDefaultTimeout(new DescribeTableSettings());
        String databaseWithSuffix = withSuffix(connection.getCtx().getDatabase());

        try (Session session = executor.createSession(connection.getCtx())) {
            try {
                return executor.call("Describe table " + table,
                        () -> session.describeTable(databaseWithSuffix + table, settings)
                );
            } catch (YdbStatusException ex) {
                if (ex.getStatus().getCode() != StatusCode.SCHEME_ERROR) { // ignore scheme errors like path not found
                    throw ex;
                }
                LOGGER.log(Level.WARNING, "Cannot describe table {0} -> {1}",
                        new Object[]{ table, ex.getMessage() }
                );

                return null;
            }
        }
    }

    private ResultSet emptyResultSet(FixedResultSetFactory factory) {
        YdbStatementImpl statement = new YdbStatementImpl(connection, ResultSet.TYPE_SCROLL_INSENSITIVE);
        return new YdbResultSetImpl(statement, factory.createResultSet().build());
    }

    private ResultSet resultSet(ResultSetReader rsReader) {
        YdbStatementImpl statement = new YdbStatementImpl(connection, ResultSet.TYPE_SCROLL_INSENSITIVE);
        return new YdbResultSetImpl(statement, rsReader);
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
