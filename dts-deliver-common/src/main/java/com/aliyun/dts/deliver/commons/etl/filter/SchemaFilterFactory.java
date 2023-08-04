package com.aliyun.dts.deliver.commons.etl.filter;

import com.aliyun.dts.deliver.commons.etl.impl.WhiteList;

import java.util.Set;

public class SchemaFilterFactory {

    private static SchemaCondition createSchemaCondition(String conditionList, String attachedColumnList) {
        UnionSchemaCondition conditions = null;

        if (conditionList != null && conditionList.trim().length() > 0) {
            conditions = new UnionSchemaCondition();

            String[] sqls = conditionList.split("\\|");
            for (String sql : sqls) {
                String[] ss = sql.split(";");
                if (ss.length < 3) {
                    continue;
                }
                RegularSchemaCondition condition = new RegularSchemaCondition(ss[0], ss[1], ss[2]);
                conditions.addCondition(condition);
            }
        }

        if (attachedColumnList != null && attachedColumnList.trim().length() > 0) {
            if (conditions == null) {
                conditions = new UnionSchemaCondition();
            }

            String[] columns = attachedColumnList.split("\\|");
            for (String column : columns) {
                String[] ss = column.split(";");
                if (ss.length < 5) {
                    continue;
                }

                conditions.addAttachedColumn(ss[0], ss[0], ss[1], new AttachedColumn(ss[2], ss[3], ss[4]));
            }
        }

        return conditions;
    }

    private static SchemaOperations createSchemaOperations(String operationsList) {
        UnionSchemaOperations operations = null;

        if (operationsList != null && operationsList.trim().length() > 0) {
            operations = new UnionSchemaOperations();
            String[] sqls = operationsList.split("\\|");
            for (String sql : sqls) {
                String[] ss = sql.split(";");
                if (ss.length < 4) {
                    continue;
                }

                RegularSchemaOperations operation = new RegularSchemaOperations(ss[0], ss[1], ss[2], ss[3]);
                operations.addSchemaOperation(operation);
            }
        }

        return operations;
    }

    @Deprecated
    public static SchemaFilter createRegularSchemaFilter(String conditionList, String attachedColumnList, String line,
                                                         boolean casesensitived, String operationsList) {
        return createSchemaFilter(line, conditionList, attachedColumnList, operationsList, "mysql", "mysql",
            false, true, casesensitived);
    }

    @Deprecated
    public static SchemaFilter createJsonSchemaFilter(String type, String line, boolean casesensitived) {
        return createSchemaFilter(line, null, null, null, type, type,
            true, true, casesensitived);
    }

    @Deprecated
    public static SchemaFilter createJsonSchemaFilter(String srcType, String destType, String line, boolean casesensitived) {
        return createSchemaFilter(line, null, null, null, srcType, destType,
            true, true, casesensitived);
    }

    @Deprecated
    public static SchemaFilter createSchemaFilter(String line, String conditionList, String attachedColumnList, String operationsList,
                                                  String srcDbType, String destDbType, boolean isJson, boolean isWhitelist,
                                                  boolean caseSensitive) {
        return createSchemaFilter(line, conditionList, attachedColumnList, operationsList,
                srcDbType, destDbType, isJson, isWhitelist, caseSensitive, 0, "default", "without.schema");
    }

    public static SchemaFilter createSchemaFilter(String line, String conditionList, String attachedColumnList, String operationsList,
                                                  String srcDbType, String destDbType, boolean isJson, boolean isWhitelist,
                                                  boolean caseSensitive, int destLowerCaseTableNames, String dbListCaseChangeMode, String schemaMapperMode) {

        FilterMode filterMode = isWhitelist ? FilterMode.WHITELIST : FilterMode.BLACKLIST;

        if (isJson) {
            // if dbList is null then Transfer all DBs.
            if ((line == null) || line.equals("")) {
                return null;
            }
            SchemaFilter filter = new JsonSchemaFilter(line, srcDbType, destDbType, filterMode, caseSensitive, destLowerCaseTableNames, dbListCaseChangeMode, schemaMapperMode);
            return filter;
        } else {
            if (line == null || line.trim().length() <= 0) {
                return null;
            }

            SchemaCondition conditions = createSchemaCondition(conditionList, attachedColumnList);
            SchemaOperations operations = createSchemaOperations(operationsList);

            String[] lines = line.split("\\|");

            UnionSchemaFilter unionFilter = new UnionSchemaFilter(null, conditions, operations, caseSensitive);
            if (!isWhitelist) {
                unionFilter = new JoinSchemaFilter(caseSensitive);
            }

            for (String s : lines) {
                String[] ss = s.split(";");
                if (ss.length < 3) {
                    continue;
                }

                RegularSchemaFilter filter = new RegularSchemaFilter(ss[0], ss[1], ss[2], filterMode);
                unionFilter.addFilter(filter);
            }

            return unionFilter;
        }
    }

    @Deprecated
    public static Set<String> databases() {
        return WhiteList.allDatabases();
    }

    @Deprecated
    public static String tableParentId(String database, String table) {
        return WhiteList.tableParentId(database, table);
    }

    @Deprecated
    public static String tablePartitionKey(String database, String table) {
        return WhiteList.tablePartitionKey(database, table);
    }

    @Deprecated
    public static String[] tableIdValues(String database, String table) {
        String[] docIdNames = null;

        String value = WhiteList.tableIdValue(database, table);

        if (value != null) {
            docIdNames = value.split(",");
        }

        return docIdNames == null || docIdNames.length <= 0 ? null : docIdNames;
    }
}
