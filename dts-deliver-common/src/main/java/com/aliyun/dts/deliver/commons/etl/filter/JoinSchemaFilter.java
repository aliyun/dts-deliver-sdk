package com.aliyun.dts.deliver.commons.etl.filter;

import java.util.List;

public class JoinSchemaFilter extends UnionSchemaFilter {

    public JoinSchemaFilter() {
        this(true);
    }

    public JoinSchemaFilter(boolean casesensitive) {
        this(null, null, null, casesensitive);
    }

    public JoinSchemaFilter(List<SchemaFilter> filters, SchemaCondition conditions, SchemaOperations operations,
                            boolean casesensitive) {
        super(filters, conditions, operations, casesensitive);
    }

    @Override
    public boolean shouldIgnore(String schema, String database) {
        for (SchemaFilter schemaFilter : filters) {
            if (schemaFilter.shouldIgnore(schema, database)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table) {
        for (SchemaFilter schemaFilter : filters) {
            if (schemaFilter.shouldIgnore(schema, database, table)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table, String column) {
        for (SchemaFilter schemaFilter : filters) {
            if (schemaFilter.shouldIgnore(schema, database, table, column)) {
                return true;
            }
        }

        return false;
    }
}
