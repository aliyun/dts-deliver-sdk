package com.aliyun.dts.deliver.commons.etl.filter;

import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;

import java.util.List;

public class RegularSchemaCondition implements SchemaCondition {

    private String database;
    private String table;
    private String sql;

    public RegularSchemaCondition(String database, String table, String sql) {
        this.database = database;
        this.table = table;
        this.sql = sql;
    }

    private boolean filter(String schema, String database, String table) {

        if (database == null || table == null) {
            return true;
        }

        return !database.matches(this.database) || !table.matches(this.table);
    }

    public String condition(String schema, String database, String table) {
        if (this.filter(schema, database, table)) {
            return null;
        }

        return this.sql;
    }

    @Override
    public List<AttachedColumn> attachedColumns(String schema, String database, String table) {
        throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_UNSUPPORTED_OPERATION, "RegularSchemaCondition.attachedColumns Can not ben implemented");
    }

    public static void main(String[] args) {
        RegularSchemaCondition condition = new RegularSchemaCondition("^wireless_amp_sync_0023", "^sync_message_.*", "gmt_modified>1538668800000");

        System.out.println(condition.condition("wireless_amp_sync_0023", "wireless_amp_sync_0023", "sync_message_0755"));
    }
}
