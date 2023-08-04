package com.aliyun.dts.deliver.protocol.record.impl.jdbc;

import com.aliyun.dts.deliver.protocol.record.RawDataType;
import com.aliyun.dts.deliver.protocol.record.impl.DefaultRawDataType;

import java.sql.Types;

public interface JdbcRawDataTypes {
    int CUSTOMIZED_TYPE_START_POINT = 4096;

    RawDataType BOOLEAN = DefaultRawDataType.of("BOOLEAN", Types.BOOLEAN);
    RawDataType SHORT = DefaultRawDataType.of("SHORT", Types.SMALLINT);
    RawDataType TINYINT = DefaultRawDataType.of("TINYINT", Types.TINYINT);
    RawDataType INTEGER = DefaultRawDataType.of("INTEGER", Types.INTEGER);
    RawDataType BIGINT = DefaultRawDataType.of("BIGINT", Types.BIGINT);
    RawDataType FLOAT = DefaultRawDataType.of("FLOAT", Types.FLOAT);
    RawDataType DOUBLE = DefaultRawDataType.of("DOUBLE", Types.DOUBLE);
    RawDataType BIT = DefaultRawDataType.of("BIT", Types.BIT);
    RawDataType DATE = DefaultRawDataType.of("DATE", Types.DATE);
    RawDataType TIME = DefaultRawDataType.of("TIME", Types.TIME);
    RawDataType TIME_WITH_TIMEZONE = DefaultRawDataType.of("TIME_WITH_TIMEZONE", Types.TIME_WITH_TIMEZONE);
    RawDataType TIMESTAMP = DefaultRawDataType.of("TIMESTAMP", Types.TIMESTAMP);
    RawDataType TIMESTAMP_WITH_TIMEZONE = DefaultRawDataType.of("TIMESTAMP_WITH_TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE);
    RawDataType TIMESTAMPLOCALZONE = DefaultRawDataType.of("TIMESTAMPLOCALZONE", JdbcTypes.TIMESTAMPLOCALZONE);
    RawDataType BYTES = DefaultRawDataType.of("BYTES", Types.BINARY, true);
    RawDataType BLOB = DefaultRawDataType.of("BLOB", Types.BLOB, true);
    RawDataType STRING = DefaultRawDataType.of("STRING", Types.CHAR);
    RawDataType DECIMAL = DefaultRawDataType.of("DECIMAL", Types.DECIMAL);

    RawDataType XML = DefaultRawDataType.of("XML", Types.SQLXML);
    RawDataType JSON = DefaultRawDataType.of("JSON", CUSTOMIZED_TYPE_START_POINT);
    RawDataType GEOMETRY = DefaultRawDataType.of("GEOMETRY", CUSTOMIZED_TYPE_START_POINT + 1);
    RawDataType DATETIME = DefaultRawDataType.of("DATETIME", CUSTOMIZED_TYPE_START_POINT + 2);
    RawDataType YEAR = DefaultRawDataType.of("YEAR", CUSTOMIZED_TYPE_START_POINT + 3);
    RawDataType STRUCT = DefaultRawDataType.of("STRUCT", Types.OTHER);

    RawDataType MONEY = DefaultRawDataType.of("MONEY", JdbcTypes.X_POSTGRESQL_MONEY);
    RawDataType XTYPES = DefaultRawDataType.of("XTYPES", JdbcTypes.POSTGRESQL_XTYPES);

    RawDataType INTERVALDS = DefaultRawDataType.of("INTERVALDS", JdbcTypes.INTERVALDS);
    RawDataType INTERVALYM = DefaultRawDataType.of("INTERVALYM", JdbcTypes.INTERVALYM);

    RawDataType CUSTOM_TYPE = DefaultRawDataType.of("CUSTOM_TYPE", JdbcTypes.X_POSTGRESQL_CUSTOM_TYPE);
    RawDataType CLOB = DefaultRawDataType.of("CLOB", JdbcTypes.CLOB);
    RawDataType NCLOB = DefaultRawDataType.of("NCLOB", JdbcTypes.NCLOB);
    RawDataType ARRAY = DefaultRawDataType.of("ARRAY", JdbcTypes.POSTGRESQL_ARRAY);
}
