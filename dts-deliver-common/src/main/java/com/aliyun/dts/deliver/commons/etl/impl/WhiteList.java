package com.aliyun.dts.deliver.commons.etl.impl;

import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WhiteList {

    private static final Logger LOGGER = LoggerFactory.getLogger(WhiteList.class);
    private static WhiteListFacade whiteListFacade;

    // just make the user happy
    public static boolean initialized;

    public enum DB_CATEGORY {
        NORMAL, MSSQL, MONGO, ELK
    }

    // TODO sqlserver->normal support: database.schema
    public enum SCHEMA_MAPPER_MODE {
        SCHEMA_TABLE,
        DATABASE_SCHEMA,
        WITHOUT_SCHEMA,
        WITH_SCHEMA;

        static SCHEMA_MAPPER_MODE parse(String schemaMapperMode) {
            try {
                return SCHEMA_MAPPER_MODE.valueOf(schemaMapperMode.toUpperCase().replace(".", "_"));
            } catch (Exception e) {
                // do nothing
            }

            return WITHOUT_SCHEMA;
        }
    }

    public static void init(String json) {
        init(json, DB_CATEGORY.NORMAL);
    }

    public static void init(String json, DB_CATEGORY db_category) {
        init(json, db_category, db_category);
    }

    public static synchronized void init(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory) {
        init(json, srcCategory, destCategory, false);
    }


    public static synchronized void reInit(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory, boolean caseInSensitive) {
        initialized = false;
        init(json, srcCategory, destCategory, caseInSensitive);
    }

    public static synchronized void init(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory, boolean caseInSensitive) {
        init(json, srcCategory, destCategory, caseInSensitive, "", 0, "default", "without.schema");
    }

    public static synchronized void init(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory, boolean caseInSensitive,
                                         String destDbType, int destLowerCaseTableNames, String dbListCaseChangeMode, String schemaMapperMode) {
        if (initialized) {
            // already initialized, do nothing
            return;
        }

        // initialize deprecated white list
        DeprecatedWhiteList.initialized = false;
        DeprecatedWhiteList.init(json, srcCategory, destCategory);

        // initialize recursive whitelist
        WhiteListFacade tmpWhiteListFacade = new RecursiveWhiteList();
        try {
            tmpWhiteListFacade.initialize(json, srcCategory, destCategory, caseInSensitive, destDbType, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            whiteListFacade = tmpWhiteListFacade;
            initialized = true;
        } catch (Exception e) {
            LOGGER.warn("initialize whitelist failed", e);
            e.printStackTrace();
        }
    }

    enum ItemType {
        NONE,
        INSTANCE,
        SCHEMA,
        DATABASE,
        TABLE,
        COLUMN,
        VIEW,
        SYNONYM,
        PROCEDURE,
        FUNCTION,
        TYPE,
        RULE,
        PLAN,
        PACKAGE,
        SEQUENCE,
        DOMAIN,
        AGGREGATE,
        OPERATOR,
        EXTENSION,
        DEFAULT,
        JOB,
        SCRIPTS;

        static ItemType parse(String itemTypeString) {
            try {
                return ItemType.valueOf(itemTypeString.toUpperCase());
            } catch (Exception e) {
                // do nothing
            }
            return NONE;
        }
    }

    public interface WhiteListFacade {

        default void initialize(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory, boolean sourceCaseInSensitive) throws Exception {
            initialize(json, srcCategory, destCategory, sourceCaseInSensitive, "", 0, "default", "without.schema");
        }

        void initialize(String json, DB_CATEGORY srcCategory, DB_CATEGORY destCategory, boolean sourceCaseInSensitive, String destDbType, int lowerCaseTableNames,
                        String dbListCaseChangeMode, String schemaMapperMode) throws Exception;

        /******** Add white list in json to current white list ********/
        void addWhiteListItem(String json) throws Exception;

        /******** Following is filter funcitons ********/
        Boolean shouldIgnore(String database, String table, String column);

        /******** Following is mapper functions ********/
        String dbMapper(String database, String table, String column);

        String itemMapper(String database, String table, ItemType itemType);

        /********* Following is extra attribute getters ********/
        Object getExtraAttribute(String database, String table, String column, String attributeName);

        /********* Following is script related getters *********/
        Optional<Pair<String, String>> getScriptInfo(String database, String table);

        List<Pair<String, String>> getAllScriptInfo();

        Pair<String, String> getScriptContent(String scriptNamespace, String scriptName);

        /******** Flowing is deprecated function********/
        Set<String> allDatabases();

        String getColumnValue(String database, String table, String column);

        RecursiveWhiteList.PatternType getRawPatternType(String database, String table);
    }

    /******** The methods below are all deprecated ********/
    public static Set<String> allDatabases() {
        return getFacade().allDatabases();
    }

    public static Set<String> allLowerCaseDatabases() {
        return DeprecatedWhiteList.allLowerCaseDatabases();
    }

    public static Set<String> getTables(String database) {
        return DeprecatedWhiteList.getTables(database);
    }

    public static Set<String> getLowerCaseDbTables(String database) {
        return DeprecatedWhiteList.getLowerCaseDbTables(database);
    }

    public static Set<String> getDestSchemas(String database) {
        return DeprecatedWhiteList.getDestSchemas(database);
    }

    public static String getTablegroup(String database) {
        return DeprecatedWhiteList.getTablegroup(database);
    }

    public static String getFilter(String database, String table) {
        return DeprecatedWhiteList.getFilter(database, table);
    }

    public static Object getFilter(String database, String table, DB_CATEGORY category) {
        return DeprecatedWhiteList.getFilter(database, table, category);
    }

    public static String getFamily(String database, String table) {
        return DeprecatedWhiteList.getFamily(database, table);
    }

    public static Set<String> getColumns(String table) {
        return DeprecatedWhiteList.getColumns(table);
    }

    public static Set<String> getColumns(String database, String table) {
        return DeprecatedWhiteList.getColumns(database, table);
    }

    public static Set<String> getLowerCaseColumns(String database, String table) {
        return DeprecatedWhiteList.getLowerCaseColumns(database, table);
    }

    public static String getColumnType(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnType(database, table, column);
    }

    public static String getColumnDefaultValue(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnDefaultValue(database, table, column);
    }

    public static Boolean hasAll(String database) {
        return DeprecatedWhiteList.hasAll(database);
    }

    public static Boolean hasLowerCaseAll(String database) {
        return DeprecatedWhiteList.hasLowerCaseAll(database);
    }

    public static Boolean caseSen(String database) {
        return DeprecatedWhiteList.caseSen(database);
    }

    public static Boolean hasAll(String database, String table) {
        return DeprecatedWhiteList.hasAll(database, table);
    }

    public static Boolean hasLowerCaseAll(String database, String table) {
        return DeprecatedWhiteList.hasLowerCaseAll(database, table);
    }

    public static String dbMapper(String database) {
        return getFacade().dbMapper(database, null, null);
    }

    public static String dbMapper(String database, String table) {
        return getFacade().dbMapper(database, table, null);
    }

    public static String dbMapper(String database, String table, String column) {
        return getFacade().dbMapper(database, table, column);
    }

    public static Set<String> getViews(String database) {
        return DeprecatedWhiteList.getViews(database);
    }

    public static Set<String> getSynonym(String database) {
        return DeprecatedWhiteList.getSynonym(database);
    }

    public static Set<String> getProcedure(String database) {
        return DeprecatedWhiteList.getProcedure(database);
    }

    public static Set<String> getFunction(String database) {
        return DeprecatedWhiteList.getFunction(database);
    }

    public static Set<String> getType(String database) {
        return DeprecatedWhiteList.getType(database);
    }

    public static Set<String> getRule(String database) {
        return DeprecatedWhiteList.getRule(database);
    }

    public static Set<String> getDefault(String database) {
        return DeprecatedWhiteList.getDefault(database);
    }

    public static Set<String> getPlan(String database) {
        return DeprecatedWhiteList.getPlan(database);
    }

    public static Set<String> getPackage(String database) {
        return DeprecatedWhiteList.getPackage(database);
    }

    public static Set<String> getSequence(String database) {
        return DeprecatedWhiteList.getSequence(database);
    }

    public static Set<String> getDomain(String database) {
        return DeprecatedWhiteList.getDomain(database);
    }

    public static Set<String> getAggregate(String database) {
        return DeprecatedWhiteList.getAggregate(database);
    }

    public static Set<String> getOperator(String database) {
        return DeprecatedWhiteList.getOperator(database);
    }

    public static Set<String> getExtension(String database) {
        return DeprecatedWhiteList.getExtension(database);
    }

    public static Set<String> getJob(String database) {
        return DeprecatedWhiteList.getJob(database);
    }

    public static String dbDmlOperations(String database) {
        return DeprecatedWhiteList.dbDmlOperations(database);
    }

    public static String dbDdlOperations(String database) {
        return DeprecatedWhiteList.dbDdlOperations(database);
    }

    public static String viewMapper(String database, String table) {
        return DeprecatedWhiteList.viewMapper(database, table);
    }

    public static String synonymMapper(String database, String table) {
        return DeprecatedWhiteList.synonymMapper(database, table);
    }

    public static String procedureMapper(String database, String table) {
        return DeprecatedWhiteList.procedureMapper(database, table);
    }

    public static String functionMapper(String database, String table) {
        return DeprecatedWhiteList.functionMapper(database, table);
    }

    public static String typeMapper(String database, String table) {
        return DeprecatedWhiteList.typeMapper(database, table);
    }

    public static String ruleMapper(String database, String table) {
        return DeprecatedWhiteList.ruleMapper(database, table);
    }

    public static String defaultMapper(String database, String table) {
        return DeprecatedWhiteList.defaultMapper(database, table);
    }

    public static String planMapper(String database, String table) {
        return DeprecatedWhiteList.planMapper(database, table);
    }

    public static String packageMapper(String database, String table) {
        return DeprecatedWhiteList.packageMapper(database, table);
    }

    public static String sequenceMapper(String database, String table) {
        return DeprecatedWhiteList.sequenceMapper(database, table);
    }

    public static String domainMapper(String database, String table) {
        return DeprecatedWhiteList.domainMapper(database, table);
    }

    public static String aggregateMapper(String database, String table) {
        return DeprecatedWhiteList.aggregateMapper(database, table);
    }

    public static String operatorMapper(String database, String table) {
        return DeprecatedWhiteList.operatorMapper(database, table);
    }

    public static String extensionMapper(String database, String table) {
        return DeprecatedWhiteList.extensionMapper(database, table);
    }

    public static String dbPartionKey(String database, String table) {
        return DeprecatedWhiteList.dbPartionKey(database, table);
    }

    public static String tabPartionKey(String database, String table) {
        return DeprecatedWhiteList.tabPartionKey(database, table);
    }

    public static String tabNumEachdb(String database, String table) {
        return DeprecatedWhiteList.tabNumEachdb(database, table);
    }

    public static String tableType(String database, String table) {
        return DeprecatedWhiteList.tableType(database, table);
    }

    public static String tablePrimaryKey(String database, String table) {
        return DeprecatedWhiteList.tablePrimaryKey(database, table);
    }

    public static String tableCluster(String database, String table) {
        return DeprecatedWhiteList.tableCluster(database, table);
    }

    public static String tablePartKey(String database, String table) {
        return DeprecatedWhiteList.tablePartKey(database, table);
    }

    public static String tablePartNum(String database, String table) {
        return DeprecatedWhiteList.tablePartNum(database, table);
    }

    public static String tableDbName(String database, String table) {
        return DeprecatedWhiteList.tableDbName(database, table);
    }

    public static String tableDmlOperations(String database, String table) {
        return DeprecatedWhiteList.tableDmlOperations(database, table);
    }

    public static String tableDdlOperations(String database, String table) {
        return DeprecatedWhiteList.tableDdlOperations(database, table);
    }

    public static String tablePartitions(String database, String table) {
        return DeprecatedWhiteList.tablePartitions(database, table);
    }

    public static String dbConfilct(String database) {
        return DeprecatedWhiteList.dbConfilct(database);
    }

    public static String tableConfilct(String database, String table) {
        return DeprecatedWhiteList.tableConfilct(database, table);
    }

    public static Integer dbShard(String database) {
        return DeprecatedWhiteList.dbShard(database);
    }

    public static Integer tableShard(String database, String table) {
        return DeprecatedWhiteList.tableShard(database, table);
    }

    public static Integer dbReplica(String database) {
        return DeprecatedWhiteList.dbReplica(database);
    }

    public static String dbAnalysis(String database) {
        return DeprecatedWhiteList.dbAnalysis(database);
    }

    public static String dbAnalyzer(String database) {
        return DeprecatedWhiteList.dbAnalyzer(database);
    }

    public static String dbTimezone(String database) {
        return DeprecatedWhiteList.dbTimezone(database);
    }

    public static Boolean tableIsPartition(String database, String table) {
        return DeprecatedWhiteList.tableIsPartition(database, table);
    }

    public static String tablePartitionKey(String database, String table) {
        return DeprecatedWhiteList.tablePartitionKey(database, table);
    }

    public static String tableId(String database, String table) {
        return DeprecatedWhiteList.tableId(database, table);
    }

    public static String tableIdValue(String database, String table) {
        return DeprecatedWhiteList.tableIdValue(database, table);
    }

    public static Boolean tableIsJoin(String database, String table) {
        return DeprecatedWhiteList.tableIsJoin(database, table);
    }

    public static String tableRelationRole(String database, String table) {
        return DeprecatedWhiteList.tableRelationRole(database, table);
    }

    public static String tableParentName(String database, String table) {
        return DeprecatedWhiteList.tableParentName(database, table);
    }

    public static String tableParentId(String database, String table) {
        return DeprecatedWhiteList.tableParentId(database, table);
    }

    public static String tableIndex(String database, String table) {
        return DeprecatedWhiteList.tableIndex(database, table);
    }

    public static String dbMessageKey(String database) {
        return DeprecatedWhiteList.dbMessageKey(database);
    }

    public static String tableMessageKey(String database, String table) {
        return DeprecatedWhiteList.tableMessageKey(database, table);
    }

    public static Boolean getColumnIndexValue(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnIndexValue(database, table, column);
    }

    public static String getColumnAnalyzer(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnAnalyzer(database, table, column);
    }

    public static String getColumnTargetType(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnTargetType(database, table, column);
    }

    public static String dbIndexMapping(String database) {
        return DeprecatedWhiteList.dbIndexMapping(database);
    }

    public static Set<String> getTopicPartitionKeys(String database, String table) {
        return DeprecatedWhiteList.getTopicPartitionKeys(database, table);
    }

    public static Map<String, String> getTableFilters() {
        return DeprecatedWhiteList.table_filter;
    }

    public static String getColumnValue(String database, String table, String column) {
        return getFacade().getColumnValue(database, table, column);
    }

    public static String getRawColumnValue(String database, String table, String column) {
        return DeprecatedWhiteList.getColumnValue(database, table, column);
    }

    public static RecursiveWhiteList.PatternType getRawPatternType(String database, String table) {
        return  getFacade().getRawPatternType(database, table);
    }

    /******** We should use this interface in future ********/
    public static WhiteListFacade getFacade() {
        return whiteListFacade;
    }

    public static synchronized void setDMSOnlineDDLTablePattern(boolean isEnabled) throws Exception {
        final String filterType = isEnabled ? "gray" : "black";
        String tablePatternTemplate = "{" +
            "  \"%s\": {" +
            "    \"all\": false," +
            "    \"Table\": {" +
            "      \"tp_\\\\d+_[ogl|del|ogt|d|g|l]{1,3}_(.*)\": {" +
            "        \"name\": \"{0}\"," +
            "        \"patternType\": \"regex\"," +
            "        \"filterType\": \"%s\"," +
            "        \"grayName\": \"{1}\"," +
            "        \"grayNameMatchWay\": \"exact\"," +
            "        \"all\": true" +
            "      }" +
            "    }" +
            "  }" +
            "}";
        Set<String> databases = allDatabases();
        for (String database : databases) {
            String tablePattern = String.format(tablePatternTemplate, database, filterType);
            getFacade().addWhiteListItem(tablePattern);
        }
    }
}
