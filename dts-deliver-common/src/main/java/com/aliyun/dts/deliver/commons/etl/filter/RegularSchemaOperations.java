package com.aliyun.dts.deliver.commons.etl.filter;

public class RegularSchemaOperations implements SchemaOperations {
    private String database;
    private String table;
    private String dmlOperations;
    private String ddlOperations;

    public RegularSchemaOperations(String database, String table, String dmlOperations, String ddlOperations) {
        this.database = database;
        this.table = table;
        this.dmlOperations = dmlOperations;
        this.ddlOperations = ddlOperations;
    }

    private boolean filter(String schema, String database, String table) {
        if (database == null || table == null) {
            return true;
        }

        return !database.matches(this.database) || !table.matches(this.table);
    }

    @Override
    public String dmlOperations(String schema, String database, String table) {
        if (this.filter(schema, database, table)) {
            return null;
        }

        return this.dmlOperations;
    }

    @Override
    public String ddlOperations(String schema, String database, String table) {
        if (this.filter(schema, database, table)) {
            return null;
        }

        return this.ddlOperations;
    }
}
