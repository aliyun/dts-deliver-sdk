package com.aliyun.dts.deliver.commons.etl.filter;

public interface SchemaOperations {
    String dmlOperations(String schema, String database, String table);

    String ddlOperations(String schema, String database, String table);
}
