package com.aliyun.dts.deliver.commons.etl.filter;

import java.util.List;

public interface SchemaCondition {

    String condition(String schema, String database, String table);

    List<AttachedColumn> attachedColumns(String schema, String database, String table);
}
