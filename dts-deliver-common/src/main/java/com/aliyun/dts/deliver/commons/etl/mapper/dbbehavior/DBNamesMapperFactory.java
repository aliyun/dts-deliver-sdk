package com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior;

import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class DBNamesMapperFactory {

    static final Map<String, String> DB_TYPE_ALIAS = new HashMap<>();

    static {
        DB_TYPE_ALIAS.put("SQLServer", "MSSQL");
        DB_TYPE_ALIAS.put("OB1.0", "OB10");
    }

    public static DBNamesMapper createADB30DBNamesMapper(int lowerCaseTableNames) {
        if (lowerCaseTableNames > 0) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new DefaultNameMapper());
        } else {
            return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
        }
    }

    public static DBNamesMapper createADSDBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createDB2DBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createDataHubDBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createGreenplumDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createMSSQLDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createMariaDBDBNamesMapper(int lowerCaseTableNames) {
        if (lowerCaseTableNames > 0) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new DefaultNameMapper());
        } else {
            return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
        }
    }

    public static DBNamesMapper createMongoDBDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createMySQLDBNamesMapper(int lowerCaseTableNames) {
        if (lowerCaseTableNames > 0) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new DefaultNameMapper());
        } else {
            return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
        }
    }

    public static DBNamesMapper createOB10DBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createODPSDBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createOracleDBNamesMapper() {
        return new DBNamesMapper(new UpperNameMapper(), new UpperNameMapper(), new UpperNameMapper());
    }

    public static DBNamesMapper createPPASDBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createPetaDataDBNamesMapper() {
        return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
    }

    public static DBNamesMapper createPolarDBDBNamesMapper(int lowerCaseTableNames) {
        if (lowerCaseTableNames > 0) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new DefaultNameMapper());
        } else {
            return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
        }
    }

    public static DBNamesMapper createPostgreSQLDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createRedisDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createTablestoreDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createDrdsDBNamesMapper(int lowerCaseTableNames) {
        if (lowerCaseTableNames > 0) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new DefaultNameMapper());
        } else {
            return new DBNamesMapper(new LowerNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
        }
    }

    public static DBNamesMapper createElkDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createKafkaDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createPolardb_ODBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper creatDefaultDBNamesMapper() {
        return new DBNamesMapper(new DefaultNameMapper(), new DefaultNameMapper(), new DefaultNameMapper());
    }

    public static DBNamesMapper createDBNamesMapper(String dbType, int lowerCaseTableNames, String dbListCaseChangeMode) {
        if ("source".equalsIgnoreCase(dbListCaseChangeMode)) {
            return creatDefaultDBNamesMapper();
        } else if ("dest_upper".equalsIgnoreCase(dbListCaseChangeMode)) {
            return new DBNamesMapper(new UpperNameMapper(), new UpperNameMapper(), new UpperNameMapper());
        } else if ("dest_lower".equalsIgnoreCase(dbListCaseChangeMode)) {
            return new DBNamesMapper(new LowerNameMapper(), new LowerNameMapper(), new LowerNameMapper());
        } else {
            return createDBNamesMapper(dbType, lowerCaseTableNames);
        }
    }

    public static DBNamesMapper createDBNamesMapper(String dbType, int lowerCaseTableNames) {
        try {
            Method[] methods = DBNamesMapperFactory.class.getDeclaredMethods();
            Method createMethod = null;
            if (DB_TYPE_ALIAS.containsKey(dbType)) {
                dbType = DB_TYPE_ALIAS.get(dbType);
            }
            String name = "create" + dbType + "DBNamesMapper";
            for (int i = 0; i < methods.length; ++i) {
                if (name.equalsIgnoreCase(methods[i].getName())) {
                    createMethod = methods[i];
                    break;
                }
            }
            if (null == createMethod) {
                return creatDefaultDBNamesMapper();
            } else {
                if (createMethod.getParameterCount() > 0) {
                    Object mapperBehavior = createMethod.invoke(null, lowerCaseTableNames);
                    return (DBNamesMapper) mapperBehavior;
                } else {
                    Object mapperBehavior = createMethod.invoke(null);
                    return (DBNamesMapper) mapperBehavior;
                }
            }
        } catch (Exception e) {
            throw new CriticalDtsException("common", ErrorCode.COMMON_LIB_CREATE_DB_NAMES_MAPPER_ERROR, e);
        }
    }
}
