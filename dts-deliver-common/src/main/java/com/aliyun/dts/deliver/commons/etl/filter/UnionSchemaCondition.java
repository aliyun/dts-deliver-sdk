package com.aliyun.dts.deliver.commons.etl.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnionSchemaCondition implements SchemaCondition {

    private List<SchemaCondition> conditions = new ArrayList<SchemaCondition>();

    private Map<String, List<AttachedColumn>> attachedColumns = new HashMap<String, List<AttachedColumn>>();

    public void addCondition(SchemaCondition condition) {
        if (condition != null) {
            this.conditions.add(condition);
        }
    }

    @Override
    public String condition(String schema, String database, String table) {

        for (SchemaCondition condition : this.conditions) {
            String sql = condition.condition(schema, database, table);
            if (sql != null) {
                return sql;
            }
        }

        return null;
    }

    public void addAttachedColumn(String schema, String database, String table, AttachedColumn column) {
        if (column == null) {
            return;
        }

        String key = database + "." + table;
        List<AttachedColumn> columns = this.attachedColumns.get(key);
        if (columns == null) {
            columns = new ArrayList<AttachedColumn>();
            this.attachedColumns.put(key, columns);
        }

        columns.add(column);
    }

    @Override
    public List<AttachedColumn> attachedColumns(String schema, String database, String table) {
        String key = database + "." + table;
        return this.attachedColumns.get(key);
    }
}
