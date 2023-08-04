package com.aliyun.dts.deliver.commons.etl.filter;

import java.util.ArrayList;
import java.util.List;

public class UnionSchemaFilter extends SchemaFilter {

    protected List<SchemaFilter> filters;

    private SchemaCondition conditions;
    private SchemaOperations operations;

    public UnionSchemaFilter() {
        this(true);
    }

    public UnionSchemaFilter(boolean casesensitive) {
        this(null, null, null, casesensitive);
    }

    public UnionSchemaFilter(List<SchemaFilter> filters, SchemaCondition conditions, SchemaOperations operations,
                             boolean casesensitive) {
        super(FilterMode.MIXED);

        if (filters == null) {
            this.filters = new ArrayList<>();
        } else {
            this.filters = filters;
        }

        this.conditions = conditions;
        this.operations = operations;
    }

    public void addFilter(SchemaFilter filter) {
        this.filters.add(filter);
    }

    @Override
    public boolean shouldIgnore(String schema, String database) {
        for (SchemaFilter filter : filters) {
            if (!filter.shouldIgnore(schema, database)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table) {
        for (SchemaFilter filter : filters) {
            if (!filter.shouldIgnore(schema, database, table)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table, String column) {
        for (SchemaFilter filter : filters) {
            if (!filter.shouldIgnore(schema, database, table, column)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String condition(String schema, String database, String table) {

        if (this.conditions == null) {
            for (SchemaFilter schemaFilter : filters) {
                String condition = schemaFilter.condition(schema, database, table);
                if (condition != null && !condition.isEmpty()) {
                    return condition;
                }
            }
            return null;
        }

        String sql = this.conditions.condition(schema, database, table);

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
    public String conflictPolicy(String schema, String database, String table) {
        return null;
    }

    public String dmlOperations(String schema, String database, String table) {
        if (null == operations) {
            for (SchemaFilter schemaFilter : filters) {
                String dml = schemaFilter.dmlOperations(schema, database, table);
                if (dml != null && !dml.isEmpty()) {
                    return dml;
                }
            }
            return null;
        } else {
            return operations.dmlOperations(schema, database, table);
        }
    }

    public String ddlOperations(String schema, String database, String table) {
        if (null == operations) {
            for (SchemaFilter schemaFilter : filters) {
                String ddl = schemaFilter.ddlOperations(schema, database, table);
                if (ddl != null && !ddl.isEmpty()) {
                    return ddl;
                }
            }
            return null;
        } else {
            return operations.ddlOperations(schema, database, table);
        }
    }
}
