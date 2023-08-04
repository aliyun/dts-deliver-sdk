
package com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior;

public class DBNamesMapper {

    private NameMapper dbNameMapper;
    private NameMapper tableNameMapper;
    private NameMapper columnNameMapper;

    public DBNamesMapper(NameMapper dbNameMapper,
                         NameMapper tableNameMapper,
                         NameMapper columnNameMapper) {
        this.dbNameMapper = dbNameMapper;
        this.tableNameMapper = tableNameMapper;
        this.columnNameMapper = columnNameMapper;
    }

    public String mapDatabaseName(String sourceDbName) {
        return this.dbNameMapper.toTarget(sourceDbName);
    }

    public String mapTableName(String sourceTableName) {
        return this.tableNameMapper.toTarget(sourceTableName);
    }

    public String mapColumnName(String sourceColumnName) {
        return this.columnNameMapper.toTarget(sourceColumnName);
    }
}
