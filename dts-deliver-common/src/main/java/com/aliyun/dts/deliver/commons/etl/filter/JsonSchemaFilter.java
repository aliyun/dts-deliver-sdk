package com.aliyun.dts.deliver.commons.etl.filter;

import com.aliyun.dts.deliver.commons.etl.impl.WhiteList;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;

public class JsonSchemaFilter extends SchemaFilter {

    private WhiteList.DB_CATEGORY type;
    private String jsonString;

    public JsonSchemaFilter(String type, String json, boolean caseSensitive) {
        this(json, type, type, FilterMode.WHITELIST, caseSensitive, 0, "default", "without.schema");
    }

    public JsonSchemaFilter(String json, String srcDbType, String destDbType, FilterMode filterMode, boolean caseSensitive) {
        this(json, srcDbType, destDbType, filterMode, caseSensitive, 0, "default", "without.schema");
    }

    public JsonSchemaFilter(String json, String srcDbType, String destDbType, FilterMode filterMode, boolean caseSensitive, int destLowerCaseTableNames,
                            String dbListCaseChangeMode, String schemaMapperMode) {

        super(filterMode);

        if (FilterMode.WHITELIST != filterMode) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_UNSUPPORTED_OPERATION, "only support whitelist mode in dblist");
        }

        if ((srcDbType != null) && srcDbType.equalsIgnoreCase("mssql")
                && ((destDbType != null) && destDbType.equalsIgnoreCase("mssql") || (destDbType != null) && destDbType.equalsIgnoreCase("file"))) {
            WhiteList.init(json, WhiteList.DB_CATEGORY.MSSQL, WhiteList.DB_CATEGORY.MSSQL, !caseSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            this.type = WhiteList.DB_CATEGORY.MSSQL;
        } else if ((srcDbType != null) && srcDbType.equalsIgnoreCase("mssql")) {
            WhiteList.init(json, WhiteList.DB_CATEGORY.MSSQL, WhiteList.DB_CATEGORY.NORMAL, !caseSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            this.type = WhiteList.DB_CATEGORY.MSSQL;
        } else if ((srcDbType != null) && srcDbType.equalsIgnoreCase("mongodb")) {
            WhiteList.init(json, WhiteList.DB_CATEGORY.MONGO, WhiteList.DB_CATEGORY.MONGO, !caseSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            this.type = WhiteList.DB_CATEGORY.MONGO;
        } else if ((destDbType != null) && destDbType.equalsIgnoreCase("elk")) {
            WhiteList.init(json, WhiteList.DB_CATEGORY.ELK, WhiteList.DB_CATEGORY.ELK, !caseSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            this.type = WhiteList.DB_CATEGORY.ELK;
        } else {
            WhiteList.init(json, WhiteList.DB_CATEGORY.NORMAL, WhiteList.DB_CATEGORY.NORMAL, !caseSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
        }

        this.jsonString = json;
    }

    @Override
    public boolean shouldIgnore(String schema, String database) {
        checkShouldNotBeNull(schema, database);

        return WhiteList.getFacade().shouldIgnore(database, null, null);
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table) {
        checkShouldNotBeNull(schema, database, table);

        return WhiteList.getFacade().shouldIgnore(database, table, null);
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table, String column) {
        checkShouldNotBeNull(schema, database, table, column);

        if (this.type == WhiteList.DB_CATEGORY.MSSQL) {
            if (column.charAt(0) != '[') {
                // fixup the column name with quote string
                column = "[" + column + "]";
            }
        }

        return WhiteList.getFacade().shouldIgnore(database, table, column);
    }

    @Override
    public String condition(String schema, String database, String table) {
        String sql = null;

        if (this.type == WhiteList.DB_CATEGORY.MONGO) {
            Object value = WhiteList.getFilter(database, table, WhiteList.DB_CATEGORY.MONGO);
            if (value != null) {
                sql = value.toString();
            }
        } else {
            sql = WhiteList.getFilter(database, table);
        }
        if (sql == null) {
            return null;
        }
        sql = sql.trim();
        if (sql == null || sql.length() <= 0) {
            return null;
        }
        return sql;
    }

    @Override
    public String ddlOperations(String schema, String database, String table) {
        String ops = WhiteList.tableDdlOperations(database, table);

        if (ops == null || ops.trim().length() <= 0) {
            ops = WhiteList.dbDdlOperations(database);
        }

        if (ops == null || ops.trim().length() <= 0) {
            return null;
        }

        return ops.trim();
    }

    @Override
    public String dmlOperations(String schema, String database, String table) {
        String ops = WhiteList.tableDmlOperations(database, table);

        if (ops == null || ops.trim().length() <= 0) {
            ops = WhiteList.dbDmlOperations(database);
        }

        if (ops == null || ops.trim().length() <= 0) {
            return null;
        }

        return ops.trim();
    }

    @Override
    public String conflictPolicy(String schema, String database, String table) {
        String policy = WhiteList.tableConfilct(database, table);
        if (policy == null || policy.length() <= 0) {
            policy = WhiteList.dbConfilct(database);
        }

        return policy;
    }

    public String getJsons() {
        return jsonString;
    }
}
