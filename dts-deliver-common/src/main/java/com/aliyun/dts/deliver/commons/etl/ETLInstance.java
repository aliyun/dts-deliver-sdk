package com.aliyun.dts.deliver.commons.etl;

import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.etl.filter.JoinSchemaFilter;
import com.aliyun.dts.deliver.commons.etl.filter.SchemaFilter;
import com.aliyun.dts.deliver.commons.etl.filter.SchemaFilterFactory;
import com.aliyun.dts.deliver.commons.etl.filter.UnionSchemaFilter;
import com.aliyun.dts.deliver.commons.etl.impl.WhiteList;
import com.aliyun.dts.deliver.commons.etl.mapper.DirectDBMapper;
import com.aliyun.dts.deliver.commons.etl.mapper.JsonDBMapper;
import com.aliyun.dts.deliver.commons.etl.mapper.Mapper;
import com.aliyun.dts.deliver.commons.etl.mapper.SchemaMapper;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ETLInstance {

    private static final Logger LOG = LoggerFactory.getLogger(ETLInstance.class);
    public static final Settings.Setting<Integer> LOWER_CASE_TABLE_NAMES = Settings.integerSetting(
            "etl.mapper.lower_case_table_names", "database lower table names configuration", 0);
    private UnionSchemaFilter schemaFilter = new JoinSchemaFilter();
    private SchemaMapper schemaMapper;
    private SchemaFilter filter;
    private List<String> destDBList = new ArrayList<>();
    private int pickUpIndex = 0;

    public synchronized String pickUpOneDB() {
        if (destDBList.isEmpty()) {
            return null;
        }
        int toPick = pickUpIndex;
        pickUpIndex = (pickUpIndex + 1) % destDBList.size();
        return destDBList.get(toPick);
    }

    public ETLInstance(Settings settings) {
        String whiteDB = settings.get("whiteDB");
        if (whiteDB == null) {
            whiteDB = settings.get("whiteList");
        }
        String blackDB = settings.get("blackList");

        String listDB = settings.get("dbList");
        String mapperList = settings.get("mapperList");

        String conditionList = settings.get("conditionList");
        String attachedColumnList = settings.get("attachedColumnList");
        String operationList = settings.get("operationsList");
        String dbListCaseChangeMode = settings.get("dbListCaseChangeMode");

        String srcDbType = null;
        String destType = null;
        Boolean srcCaseSensitive = true;

        if (listDB == null || listDB.trim().length() == 0) { // 旧版本的黑白名单

            schemaFilter.addFilter(
                SchemaFilterFactory.createSchemaFilter(whiteDB, conditionList, attachedColumnList, operationList, srcDbType, destType,
                    false, true, srcCaseSensitive, 0, "default", "without.schema"));
            schemaFilter.addFilter(
                SchemaFilterFactory.createSchemaFilter(blackDB, null, null, null, srcDbType,
                    destType, false, false, srcCaseSensitive, 0, "default", "without.schema"));

            schemaMapper = new DirectDBMapper();
            schemaMapper.initialize(mapperList);
        } else {
            this.filter = SchemaFilterFactory.createSchemaFilter(listDB, conditionList, attachedColumnList, operationList, srcDbType,
                destType, true, true, srcCaseSensitive, LOWER_CASE_TABLE_NAMES.getValue(settings), dbListCaseChangeMode, "without.schema");
            schemaFilter.addFilter(this.filter);
            if ("postgresql".equalsIgnoreCase(srcDbType) && "greenplum".equalsIgnoreCase(destType)) {
                schemaFilter.addFilter(
                    SchemaFilterFactory.createSchemaFilter(
                        ".*;^dts_postgres_heartbeat$;.*", null, null, null, srcDbType,
                        destType, false, false, srcCaseSensitive, 0, "default", "without.schema"));
            }

            schemaMapper = new JsonDBMapper(srcDbType, destType);
            schemaMapper.initialize(GlobalSettings.DB_MAPPER_JSON_EXPRESSIONS.getValue(settings));

            buildDestDBListFromConfig();
        }
    }

    private boolean shouldIgnoreByThisFilter(String databaseName, String tableName, String columnName,
                                             SchemaFilter schemaFilter) {
        if (null == schemaFilter) {
            return false;
        }

        if (StringUtils.isEmpty(tableName)) {
            return schemaFilter.shouldIgnore(databaseName, databaseName);
        } else if (StringUtils.isEmpty(columnName)) {
            return schemaFilter.shouldIgnore(databaseName, databaseName, tableName);
        } else {
            return schemaFilter.shouldIgnore(databaseName, databaseName, tableName, columnName);
        }
    }

    public boolean shouldIgnore(String databaseName) {
        return shouldIgnoreByThisFilter(databaseName, null, null, schemaFilter);
    }

    public boolean shouldIgnore(String databaseName, String tableName) {
        return shouldIgnoreByThisFilter(databaseName, tableName, null, schemaFilter);
    }

    public boolean shouldIgnore(String databaseName, String tableName, String columnName) {
        return shouldIgnoreByThisFilter(databaseName, tableName, columnName, schemaFilter);
    }

    public String mapDatabaseName(String databaseName) {
        if (null == schemaMapper) {
            return databaseName;
        }

        return schemaMapper.mapper(databaseName);
    }

    public String mapTableName(String databaseName, String tableName) {
        if (null == schemaMapper) {
            return tableName;
        }

        return schemaMapper.mapper(databaseName, tableName);
    }

    public String mapColumnName(String databaseName, String tableName, String columnName) {
        if (null == schemaMapper) {
            return columnName;
        }

        Mapper columnMapper = schemaMapper.getColumnMapper(databaseName, tableName);
        if (null == columnMapper) {
            return columnName;
        } else {
            return columnMapper.mapper(columnName);
        }
    }

    public String condition(String schema, String database, String table) {
        if (this.filter != null) {
            return this.filter.condition(schema, database, table);
        }
        return null;
    }

    public SchemaFilter getSchemaFilter() {
        return schemaFilter;
    }

    private void buildDestDBListFromConfig() {
        for (String srcDB : WhiteList.allDatabases()) {
            destDBList.add(WhiteList.dbMapper(srcDB));
        }
    }

    //only dblist has this option
    public boolean hasAll(String dbName) {
        return WhiteList.hasAll(dbName);
    }

    //only dblist has this option
    public boolean hasAll(String dbName, String tableName) {
        return WhiteList.hasAll(dbName, tableName);
    }

    public String conflictPolicy(String schema, String database, String table) {
        if (this.filter != null) {
            return this.filter.conflictPolicy(schema, database, table);
        }
        return null;
    }

    public Set<String> allDatabases() {
        return WhiteList.allDatabases();
    }

    public Set<String> getTables(String dbName) {
        return WhiteList.getTables(dbName);
    }

    public Set<String> getColumns(String dbName, String tableName) {
        return WhiteList.getColumns(dbName, tableName);
    }
}
