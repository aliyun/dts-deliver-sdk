package com.aliyun.dts.deliver.recordinterceptor.dmlfilter;

import java.util.HashMap;
import java.util.Map;

public class DBInformation {
    public static final DBInformation BLACK_DB_INFORMATION = new BlackDBInformation(null, null, null, null);
    String database;
    String databaseAlias;
    String schema;
    String schemaAlias;
    Map<String, TableInformation> tables;

    DBInformation(String database, String databaseAlias, String schema, String schemaAlias) {
        this.database = database;
        this.databaseAlias = databaseAlias;
        this.schema = schema;
        this.schemaAlias = schemaAlias;
        this.tables = new HashMap<String, TableInformation>();
    }

    public String databaseAlias() {
        return this.databaseAlias;
    }

    public String schemaAlias() {
        return this.schemaAlias;
    }

    public void addTable(String tableName, TableInformation table) {
        if (table != null) {
            this.tables.put(tableName, table);
        }
    }

    public TableInformation getTableInformation(String table) {
        return this.tables.get(table);
    }

    static class BlackDBInformation extends DBInformation {
        BlackDBInformation(String database, String databaseAlias, String schema, String schemaAlias) {
            super(database, databaseAlias, schema, schemaAlias);
        }

        @Override
        public void addTable(String tableName, TableInformation table) {
        }

        @Override
        public TableInformation getTableInformation(String table) {
            return TableInformation.BLACK_TABLE_INFORMATION;
        }
    }
}
