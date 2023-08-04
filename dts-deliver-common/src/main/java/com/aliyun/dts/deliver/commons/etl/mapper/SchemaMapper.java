package com.aliyun.dts.deliver.commons.etl.mapper;

public interface SchemaMapper extends Mapper {

    String mapper(String database);

    String mapper(String database, String table);

    Mapper getColumnMapper(String database, String table);
}
