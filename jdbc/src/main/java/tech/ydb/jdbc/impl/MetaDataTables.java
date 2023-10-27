package tech.ydb.jdbc.impl;

import java.sql.DatabaseMetaData;

import tech.ydb.jdbc.common.FixedResultSetFactory;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class MetaDataTables {
    /**
     * @see DatabaseMetaData#getProcedures(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory PROCEDURES = FixedResultSetFactory.newBuilder()
            .addTextColumn("PROCEDURE_CAT")
            .addTextColumn("PROCEDURE_SCHEM")
            .addTextColumn("PROCEDURE_NAME")
            .addTextColumn("reserved1")
            .addTextColumn("reserved2")
            .addTextColumn("reserved3")
            .addTextColumn("REMARKS")
            .addShortColumn("PROCEDURE_TYPE")
            .addTextColumn("SPECIFIC_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getProcedureColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory PROCEDURE_COLUMNS = FixedResultSetFactory.newBuilder()
            .addTextColumn("PROCEDURE_CAT")
            .addTextColumn("PROCEDURE_SCHEM")
            .addTextColumn("PROCEDURE_NAME")
            .addTextColumn("COLUMN_NAME")
            .addTextColumn("COLUMN_TYPE")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("TYPE_NAME")
            .addIntColumn("PRECISION")
            .addIntColumn("LENGTH")
            .addShortColumn("SCALE")
            .addShortColumn("RADIX")
            .addShortColumn("NULLABLE")
            .addTextColumn("REMARKS")
            .addTextColumn("COLUMN_DEF")
            .addIntColumn("SQL_DATA_TYPE")
            .addIntColumn("SQL_DATETIME_SUB")
            .addIntColumn("CHAR_OCTET_LENGTH")
            .addIntColumn("ORDINAL_POSITION")
            .addTextColumn("IS_NULLABLE")
            .addTextColumn("SPECIFIC_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
    public final static FixedResultSetFactory TABLES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("TABLE_TYPE")
            .addTextColumn("REMARKS")
            .addTextColumn("TYPE_CAT")
            .addTextColumn("TYPE_SCHEM")
            .addTextColumn("TYPE_NAME")
            .addTextColumn("SELF_REFERENCING_COL_NAME")
            .addTextColumn("REF_GENERATION")
            .build();

    /**
     * @see DatabaseMetaData#getCatalogs()
     */
    public final static FixedResultSetFactory CATALOGS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .build();

    /**
     * @see DatabaseMetaData#getTableTypes()
     */
    public final static FixedResultSetFactory TABLE_TYPES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_TYPE")
            .build();

    /**
     * @see DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory COLUMNS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("COLUMN_NAME")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("TYPE_NAME")
            .addIntColumn("COLUMN_SIZE")
            .addIntColumn("BUFFER_LENGTH")
            .addIntColumn("DECIMAL_DIGITS")
            .addIntColumn("NUM_PREC_RADIX")
            .addIntColumn("NULLABLE")
            .addTextColumn("REMARKS")
            .addTextColumn("COLUMN_DEF")
            .addIntColumn("SQL_DATA_TYPE")
            .addIntColumn("SQL_DATETIME_SUB")
            .addIntColumn("CHAR_OCTET_LENGTH")
            .addIntColumn("ORDINAL_POSITION")
            .addTextColumn("IS_NULLABLE")
            .addTextColumn("SCOPE_CATALOG")
            .addTextColumn("SCOPE_SCHEMA")
            .addTextColumn("SCOPE_TABLE")
            .addShortColumn("SOURCE_DATA_TYPE")
            .addTextColumn("IS_AUTOINCREMENT")
            .addTextColumn("IS_GENERATEDCOLUMN")
            .build();

    /**
     * @see DatabaseMetaData#getColumnPrivileges(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory COLUMN_PRIVILEGES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("COLUMN_NAME")
            .addTextColumn("GRANTOR")
            .addTextColumn("GRANTEE")
            .addTextColumn("PRIVILEGE")
            .addTextColumn("IS_GRANTABLE")
            .build();

    /**
     * @see DatabaseMetaData#getTablePrivileges(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory TABLE_PRIVILEGES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("GRANTOR")
            .addTextColumn("GRANTEE")
            .addTextColumn("PRIVILEGE")
            .addTextColumn("IS_GRANTABLE")
            .build();

    /**
     * @see DatabaseMetaData#getBestRowIdentifier(java.lang.String, java.lang.String, java.lang.String, int, boolean)
     */
    public final static FixedResultSetFactory BEST_ROW_IDENTIFIERS = FixedResultSetFactory.newBuilder()
            .addShortColumn("SCOPE")
            .addTextColumn("COLUMN_NAME")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("TYPE_NAME")
            .addIntColumn("COLUMN_SIZE")
            .addIntColumn("BUFFER_LENGTH")
            .addShortColumn("DECIMAL_DIGITS")
            .addShortColumn("PSEUDO_COLUMN")
            .build();

    /**
     * @see DatabaseMetaData#getVersionColumns(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory VERSION_COLUMNS = FixedResultSetFactory.newBuilder()
            .addShortColumn("SCOPE")
            .addTextColumn("COLUMN_NAME")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("TYPE_NAME")
            .addIntColumn("COLUMN_SIZE")
            .addIntColumn("BUFFER_LENGTH")
            .addShortColumn("DECIMAL_DIGITS")
            .addShortColumn("PSEUDO_COLUMN")
            .build();

    /**
     * @see DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory PRIMARY_KEYS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("COLUMN_NAME")
            .addShortColumn("KEY_SEQ")
            .addTextColumn("PK_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getImportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory IMPORTED_KEYS = FixedResultSetFactory.newBuilder()
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("PKTABLE_SCHEM")
            .addTextColumn("PKTABLE_NAME")
            .addTextColumn("PKCOLUMN_NAME")
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("FKTABLE_SCHEM")
            .addTextColumn("FKTABLE_NAME")
            .addTextColumn("FKCOLUMN_NAME")
            .addShortColumn("KEY_SEQ")
            .addShortColumn("UPDATE_RULE")
            .addShortColumn("DELETE_RULE")
            .addTextColumn("FK_NAME")
            .addTextColumn("PK_NAME")
            .addShortColumn("DEFERRABILITY")
            .build();

    /**
     * @see DatabaseMetaData#getExportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory EXPORTED_KEYS = FixedResultSetFactory.newBuilder()
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("PKTABLE_SCHEM")
            .addTextColumn("PKTABLE_NAME")
            .addTextColumn("PKCOLUMN_NAME")
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("FKTABLE_SCHEM")
            .addTextColumn("FKTABLE_NAME")
            .addTextColumn("FKCOLUMN_NAME")
            .addShortColumn("KEY_SEQ")
            .addShortColumn("UPDATE_RULE")
            .addShortColumn("DELETE_RULE")
            .addTextColumn("FK_NAME")
            .addTextColumn("PK_NAME")
            .addShortColumn("DEFERRABILITY")
            .build();

    /**
     * @see DatabaseMetaData#getCrossReference(java.lang.String, java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory CROSS_REFERENCES = FixedResultSetFactory.newBuilder()
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("PKTABLE_SCHEM")
            .addTextColumn("PKTABLE_NAME")
            .addTextColumn("PKCOLUMN_NAME")
            .addTextColumn("PKTABLE_CAT")
            .addTextColumn("FKTABLE_SCHEM")
            .addTextColumn("FKTABLE_NAME")
            .addTextColumn("FKCOLUMN_NAME")
            .addShortColumn("KEY_SEQ")
            .addShortColumn("UPDATE_RULE")
            .addShortColumn("DELETE_RULE")
            .addTextColumn("FK_NAME")
            .addTextColumn("PK_NAME")
            .addShortColumn("DEFERRABILITY")
            .build();

    /**
     * @see DatabaseMetaData#getTypeInfo()
     */
    public final static FixedResultSetFactory TYPE_INFOS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TYPE_NAME")
            .addIntColumn("DATA_TYPE")
            .addIntColumn("PRECISION")
            .addTextColumn("LITERAL_PREFIX")
            .addTextColumn("LITERAL_SUFFIX")
            .addTextColumn("CREATE_PARAMS")
            .addShortColumn("NULLABLE")
            .addBooleanColumn("CASE_SENSITIVE")
            .addShortColumn("SEARCHABLE")
            .addBooleanColumn("UNSIGNED_ATTRIBUTE")
            .addBooleanColumn("FIXED_PREC_SCALE")
            .addBooleanColumn("AUTO_INCREMENT")
            .addTextColumn("LOCAL_TYPE_NAME")
            .addShortColumn("MINIMUM_SCALE")
            .addShortColumn("MAXIMUM_SCALE")
            .addIntColumn("SQL_DATA_TYPE")
            .addIntColumn("SQL_DATETIME_SUB")
            .addIntColumn("NUM_PREC_RADIX")
            .build();

    /**
     * @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     */
    public final static FixedResultSetFactory INDEX_INFOS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addBooleanColumn("NON_UNIQUE")
            .addTextColumn("INDEX_QUALIFIER")
            .addTextColumn("INDEX_NAME")
            .addShortColumn("TYPE")
            .addShortColumn("ORDINAL_POSITION")
            .addTextColumn("COLUMN_NAME")
            .addTextColumn("ASC_OR_DESC")
            .addLongColumn("CARDINALITY")
            .addLongColumn("PAGES")
            .addTextColumn("FILTER_CONDITION")
            .build();

    /**
     * @see DatabaseMetaData#getUDTs(java.lang.String, java.lang.String, java.lang.String, int[])
     */
    public final static FixedResultSetFactory UDTS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("CLASS_NAME")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("REMARKS")
            .addShortColumn("BASE_TYPE")
            .build();

    /**
     * @see DatabaseMetaData#getSuperTypes(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory SUPER_TYPES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TYPE_CAT")
            .addTextColumn("TYPE_SCHEM")
            .addTextColumn("TYPE_NAME")
            .addTextColumn("SUPERTYPE_CAT")
            .addTextColumn("SUPERTYPE_SCHEM")
            .addTextColumn("SUPERTYPE_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getSuperTables(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory SUPER_TABLES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("SUPERTABLE_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getAttributes(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory ATTRIBUTES = FixedResultSetFactory.newBuilder()
            .addTextColumn("TYPE_CAT")
            .addTextColumn("TYPE_SCHEM")
            .addTextColumn("TYPE_NAME")
            .addTextColumn("ATTR_NAME")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("ATTR_TYPE_NAME")
            .addIntColumn("ATTR_SIZE")
            .addIntColumn("DECIMAL_DIGITS")
            .addIntColumn("NUM_PREC_RADIX")
            .addIntColumn("NULLABLE")
            .addTextColumn("REMARKS")
            .addTextColumn("ATTR_DEF")
            .addIntColumn("SQL_DATA_TYPE")
            .addIntColumn("SQL_DATETIME_SUB")
            .addIntColumn("CHAR_OCTET_LENGTH")
            .addIntColumn("ORDINAL_POSITION")
            .addTextColumn("IS_NULLABLE")
            .addTextColumn("SCOPE_CATALOG")
            .addTextColumn("SCOPE_SCHEMA")
            .addTextColumn("SCOPE_TABLE")
            .addShortColumn("SOURCE_DATA_TYPE")
            .build();

    /**
     * @see DatabaseMetaData#getSchemas(java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory SCHEMAS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_CATALOG")
            .build();

    /**
     * @see DatabaseMetaData#getClientInfoProperties()
     */
    public final static FixedResultSetFactory CLIENT_INFO_PROPERTIES = FixedResultSetFactory.newBuilder()
            .addTextColumn("NAME")
            .addIntColumn("MAX_LEN")
            .addTextColumn("DEFAULT_VALUE")
            .addTextColumn("DESCRIPTION")
            .build();

    /**
     * @see DatabaseMetaData#getFunctions(java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory FUNCTIONS = FixedResultSetFactory.newBuilder()
            .addTextColumn("FUNCTION_CAT")
            .addTextColumn("FUNCTION_SCHEM")
            .addTextColumn("FUNCTION_NAME")
            .addTextColumn("REMARKS")
            .addShortColumn("FUNCTION_TYPE")
            .addTextColumn("SPECIFIC_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getFunctionColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory FUNCTION_COLUMNS = FixedResultSetFactory.newBuilder()
            .addTextColumn("FUNCTION_CAT")
            .addTextColumn("FUNCTION_SCHEM")
            .addTextColumn("FUNCTION_NAME")
            .addTextColumn("COLUMN_NAME")
            .addShortColumn("COLUMN_TYPE")
            .addIntColumn("DATA_TYPE")
            .addTextColumn("TYPE_NAME")
            .addIntColumn("PRECISION")
            .addIntColumn("LENGTH")
            .addShortColumn("SCALE")
            .addShortColumn("RADIX")
            .addShortColumn("NULLABLE")
            .addTextColumn("REMARKS")
            .addIntColumn("CHAR_OCTET_LENGTH")
            .addIntColumn("ORDINAL_POSITION")
            .addTextColumn("IS_NULLABLE")
            .addTextColumn("SPECIFIC_NAME")
            .build();

    /**
     * @see DatabaseMetaData#getPseudoColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public final static FixedResultSetFactory PSEUDO_COLUMNS = FixedResultSetFactory.newBuilder()
            .addTextColumn("TABLE_CAT")
            .addTextColumn("TABLE_SCHEM")
            .addTextColumn("TABLE_NAME")
            .addTextColumn("COLUMN_NAME")
            .addIntColumn("DATA_TYPE")
            .addIntColumn("COLUMN_SIZE")
            .addIntColumn("DECIMAL_DIGITS")
            .addIntColumn("NUM_PREC_RADIX")
            .addTextColumn("COLUMN_USAGE")
            .addTextColumn("REMARKS")
            .addIntColumn("CHAR_OCTET_LENGTH")
            .addTextColumn("IS_NULLABLE")
            .build();
}
