
package com.aliyun.dts.deliver.commons.etl.mapper;


import com.aliyun.dts.deliver.commons.etl.impl.WhiteList;

public class JsonDBMapper implements SchemaMapper {

    private String srcDbType;
    private String destDbType;
    private String expressions;

    public JsonDBMapper(String srcDbType, String destDbType) {
        this.srcDbType = srcDbType;
        this.destDbType = destDbType;
    }

    public void initialize(String expressions) {
        this.expressions = expressions;
    }

    @Override
    public boolean contains(String original) {
        return false;
    }

    @Override
    public String mapper(String database) {
        return WhiteList.dbMapper(database);
    }

    @Override
    public String mapper(String database, String table) {

        //TODO(mowu): support elk/kafka with RecursiveWhiteList
        if ("elk".equalsIgnoreCase(this.destDbType)) { // 目标库是ElasticSearch
            String newTable = WhiteList.dbMapper(database, table);
            String type = WhiteList.tableIndex(database, table);
            if (type == null) {
                type = newTable;
            }

            return newTable + "/" + type;
        }

        if ("kafka".equalsIgnoreCase(this.destDbType)) { // 目标是kafka，如果是整库迁移则将库名作为topic，否则表名作为topic
            if (WhiteList.hasAll(database)) {
                return WhiteList.dbMapper(database) + ":" + WhiteList.dbMessageKey(database);
            } else {
                return WhiteList.dbMapper(database, table) + ":" + WhiteList.tableMessageKey(database, table);
            }
        }

        return WhiteList.dbMapper(database, table);
    }

    @Override
    public Mapper getColumnMapper(String database, String table) {
        JsonColumnMapper columnMapper = new JsonColumnMapper(this.srcDbType, this.destDbType, database, table);
        columnMapper.initialize(expressions);
        return columnMapper;
    }
}
