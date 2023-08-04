package com.aliyun.dts.deliver.recordinterceptor.dmlfilter;

import com.aliyun.dts.deliver.protocol.record.Record;

public class TableInformation {
    public static final TableInformation BLACK_TABLE_INFORMATION = new BlackTableInformation(null, null, null, null, null, null);
    String database;
    String databaseAlias;
    String schema;
    String schemaAlias;
    String table;
    String tableAlias;
    boolean insertable;
    boolean deleteable;
    boolean updateable;
    private String ddlOperations;
    TableInformation(String database, String schema, String table, String databaseAlias, String schemaAlias, String tableAlias,
                     String dmlOperations,
                     String ddlOperations) {

        this.database = database;
        this.databaseAlias = databaseAlias;
        this.schema = schema;
        this.schemaAlias = schemaAlias;
        this.table = table;
        this.tableAlias = tableAlias;

        if (dmlOperations == null || dmlOperations.trim().length() <= 0) {
            this.insertable = true;
            this.deleteable = true;
            this.updateable = true;
        } else {
            this.insertable = false;
            this.deleteable = false;
            this.updateable = false;

            dmlOperations = dmlOperations.toUpperCase();
            if (dmlOperations.contains("I")) {
                this.insertable = true;
            }

            if (dmlOperations.contains("D")) {
                this.deleteable = true;
            }

            if (dmlOperations.contains("U")) {
                this.updateable = true;
            }
        }

        this.ddlOperations = ddlOperations;
        if (this.ddlOperations != null && this.ddlOperations.length() > 0) {
            this.ddlOperations += ",";
        }
    }

    public String database() {
        return this.database;
    }

    public String databaseAlias() {
        return this.databaseAlias;
    }

    public String schema() {
        return this.schema;
    }

    public String schemaAlias() {
        return this.schemaAlias;
    }

    public String table() {
        return this.table;
    }

    public String tableAlias() {
        return this.tableAlias;
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    public boolean filter(Record record) {
        switch (record.getOperationType()) {
            case INSERT: {
                return !this.insertable;
            }
            case UPDATE: {
                return !this.updateable;
            }
            case DELETE: {
                return !this.deleteable;
            }
            default: {
                throw new UnsupportedOperationException("Can not support " + record.getOperationType() + " for Record Filter");
            }
        }
    }

    static class BlackTableInformation extends TableInformation {
        BlackTableInformation(String database, String schema, String table, String databaseAlias, String schemaAlias, String tableAlias) {
            super(database, schema, table, databaseAlias, schemaAlias, tableAlias,
                    null, null);
        }
    }
}
