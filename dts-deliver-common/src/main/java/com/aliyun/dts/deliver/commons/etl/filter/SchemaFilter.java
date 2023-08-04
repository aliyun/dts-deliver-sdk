package com.aliyun.dts.deliver.commons.etl.filter;

import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

public abstract class SchemaFilter {

    protected final FilterMode filterMode;

    public SchemaFilter(FilterMode filterMode) {
        this.filterMode = filterMode;
    }

    public abstract boolean shouldIgnore(String schema, String database);

    public abstract boolean shouldIgnore(String schema, String database, String table);

    public abstract boolean shouldIgnore(String schema, String database, String table, String column);

    public abstract String condition(String schema, String database, String table);

    public String dmlOperations(String schema, String database, String table) {
        return null;
    }

    public String ddlOperations(String schema, String database, String table) {
        return null;
    }

    public String conflictPolicy(String schema, String database, String table) {
        return null;
    }

    public final void checkShouldNotBeNull(String schema, String database) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(database)) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "schema name and database name should not be empty");
        }
    }

    public final void checkShouldNotBeNull(String schema, String database, String table) {
        checkShouldNotBeNull(schema, database);

        if (StringUtils.isEmpty(table)) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "table name should not be empty");
        }
    }

    public final void checkShouldNotBeNull(String schema, String database, String table, String column) {
        checkShouldNotBeNull(schema, database, table);
        if (StringUtils.isEmpty(column)) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_INVALID_PARAMETERS, "column name should not be empty");
        }
    }

    @Deprecated
    public List<AttachedColumn> attachedColumns(String schema, String database, String table) {
        return Collections.emptyList();
    }

    private boolean adjustIgnoreResult(boolean ignoreFlag, boolean black) {
        if (black) {
            if (FilterMode.BLACKLIST == filterMode) {
                return ignoreFlag;
            }
            return !ignoreFlag;
        } else {
            if (FilterMode.WHITELIST == filterMode || FilterMode.MIXED == filterMode) {
                return ignoreFlag;
            }
            return !ignoreFlag;
        }
    }

    @Deprecated
    public boolean filter(String schema, String database, boolean black) {
        boolean ignoreFlag = shouldIgnore(schema, database);
        return adjustIgnoreResult(ignoreFlag, black);
    }

    @Deprecated
    public boolean filter(String schema, String database, String table, boolean black) {
        boolean ignoreFlag = shouldIgnore(schema, database, table);
        return adjustIgnoreResult(ignoreFlag, black);
    }

    @Deprecated
    public boolean filter(String schema, String database, String table, String column, boolean black) {
        boolean ignoreFlag = shouldIgnore(schema, database, table, column);
        return adjustIgnoreResult(ignoreFlag, black);
    }
}
