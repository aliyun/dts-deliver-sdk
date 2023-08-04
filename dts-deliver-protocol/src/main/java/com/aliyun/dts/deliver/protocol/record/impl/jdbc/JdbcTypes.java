package com.aliyun.dts.deliver.protocol.record.impl.jdbc;

public class JdbcTypes {

    /**
     * BIT(n > 1)           (MySQL)
     */
    public static final int X_MYSQL_BITS = -14;
    public static final String MYSQL_BITS = "BIT";

    /**
     * BIT(n > 1)           (MySQL)
     */
    public static final int X_MYSQL_JSON = -17;
    public static final String MYSQL_JSON = "JSON";

    public static final int X_MYSQL_GEOMETRY = -18;
    public static final String MYSQL_GEOMETRY = "GEOMETRY";

    /**
     * TINYINT(1)            (MySQL)
     * TINYINT UNSIGNED(1)   (MySQL)
     */
    public static final int X_MYSQL_TINYINT = -12;
    public static final String MYSQL_TINYINT_1 = "TINYINT";
    public static final String MYSQL_TINYINT_UNSIGNED_1 = "TINYINT UNSIGNED";

    public static final String MYSQL_TINY_TEXT = "TINYTEXT";

    /**
     * BIT(1)      (MySQL)
     * bit(n)/bool (PostgreSQL)
     * bit         (SqlServer)
     */
    public static final int BIT = -7;

    /**
     * BIGINT          (MySQL)
     * BIGINT          (DB2)
     * int8/bigserial  (PostgreSQL)
     * bigint          (SqlServer)
     */
    public static final int BIGINT = -5;

    /**
     * TINYINT (MySQL)
     * tinyint (SqlServer)
     */
    public static final int TINYINT = -6;

    /**
     * SMALLINT (MySQL)
     * SMALLINT (DB2)
     * int2     (PostgreSQL)
     * smallint (SqlServer)
     */
    public static final int SMALLINT = 5;

    /**
     * MEDIUMINT/INT (MySQL)
     * INTEGER       (DB2)
     * int4/serial   (PostgreSQL)
     * int           (SqlServer)
     */
    public static final int INTEGER = 4;

    public static final int BOOLEAN = 16;

    /**
     * FLOAT   (MySQL)
     * REAL    (DB2)
     * float4/real  (PostgreSQL)
     * real    (SqlServer)
     */
    public static final int FLOAT = 7;

    public static final int REAL =  6;

    /**
     * DOUBLE       (MySQL)
     * DOUBLE       (DB2)
     * float8/money (PostgreSQL)
     * float        (SqlServer)
     */
    public static final int DOUBLE = 8;

    /**
     * BINARY_FLOAT (Oracle)
     */
    public static final int BINARY_FLOAT = 100;

    /**
     * BINARY_DOUBLE (Oracle)
     */
    public static final int BINARY_DOUBLE = 101;

    /**
     * DECIMAL(n,m)                      (MySQL)
     * DECIMAL(n,m)                      (DB2)
     * numeric                           (PostgreSQL)
     * decimal(n,m)/money/smallmoney     (SqlServer)
     */
    public static final int DECIMAL = 3;

    /**
     * NUMERIC decimal (DB2)
     * NUMBER/FLOAT(63)/FLOAT(126)  (Oracle)
     * numeric (SqlServer)
     * numeric (PostgreSQL)
     */
    public static final int NUMERIC = 2;

    /**
     * YEAR (MySQL)
     */
    public static final int X_MYSQL_YEAR = 90;
    public static final String MYSQL_YEAR = "YEAR";

    /**
     * DATE      (MySQL)
     * DATE      (DB2)
     * date      (SqlServer)
     */
    public static final int DATE = 91;

    /**
     * TIME                (MySQL)
     * TIME                (DB2)
     * time/timetz         (PostgreSQL)
     * time                (SqlServer)
     */
    public static final int TIME =  92;

    /**
     * TIMESTAMP              (MySQL)
     * TIMESTAMP              (DB2)
     * DATE/TIMESTAMP         (Oracle)
     * timestamp/timestamptz  (PostgreSQL)
     * datetime/datetime2/smalldatetime     (SqlServer)
     */
    public static final int TIMESTAMP  =  93;

    /**
     * DATETIME   (MySQL)
     */
    public static final int X_MYSQL_DATETIME = 94;
    public static final String MYSQL_DATETIME = "DATETIME";

    public static final String DATETIME_NAME = "DATETIME";
    public static final int DATETIME = 94;

    /**
     * TIMESTAMP WITH LOCAL TIME ZONE (Oracle)
     * value example:
     *  2014-05-14 01:00:00.0 Asia/Shanghai
     */
    public static final int TIMESTAMPLOCALZONE = -102;

    /**
     * TIMESTAMP WITH TIME ZONE (Oracle)
     */
    public static final int TIMESTAMPTIMEZONE = -101;

    /**
     * CHAR(n)/SET/ENUM       (MySQL)
     * GRAPHIC/CHAR  (DB2)
     * CHAR(n byte)  (Oracle)
     * bpchar        (PostgreSQL)
     * char(n)/uniqueidentifier       (SqlServer)
     *
     */
    public static final int CHAR            =   1;

    /**
     * VARCHAR(n)   (MySQL)
     * VARCHAR (DB2)
     * VARCHAR2(n)  (Oracle)
     * text/varchar (PostgreSQL)
     */
    public static final int VARCHAR         =  12;

    /**
     * ntext/xml    (SqlServer)
     */
    public static final int LONGNVARCHAR = -16;

    /**
     * VARCHAR (MySQL)
     * LONG VARGRAPHIC/LONG VARCHAR (DB2)
     * LONG (Oracle)
     * text (SqlServer)
     */
    public static final int LONGVARCHAR     =  -1;

    public static final String LONG_NAME = "LONG";

    /**
     * CLOB (Oracle)
     * CLOB/DBCLOB (DB2)
     */
    public static final int CLOB = 2005;

    /**
     * BINARY/GEOMETRY           (MySQL)
     * bytea                     (PostgreSQL)
     * binary/timestamp          (SqlServer)
     *
     */
    public static final int BINARY          =  -2;

    /**
     * VARBINARY/TINYBLOB              (MySQL)
     * RAW                              (Oracle)
     * geography/geometry/hierarchyid/varbinary   (SqlServer)
     */
    public static final int VARBINARY       =  -3;

    /**
     * BLOB (Oracle)
     * BLOB (DB2)
     */
    public static final int BLOB                = 2004;

    /**
     * BLOB/MEDIUMBLOB/LONGBLOB (MySQL)
     * image                    (SqlServer)
     */
    public static final int LONGVARBINARY   =  -4;

    /**
     * INTERVALDS (Oracle)
     */
    public static final int INTERVALDS = -104;

    /**
     * INTERVALYM (Oracle)
     */
    public static final int INTERVALYM = -103;

    /**
     * BFILE (Oracle)
     */
    public static final int BFILE = -13;

    /**
     * NCHAR (Oracle)
     * nchar (SqlServer)
     */
    public static final int NCHAR = -15;

    /**
     * NVARCHAR2 (Oracle)
     * nvarchar  (SqlServer)
     */
    public static final int NVARCHAR2 = -9;

    /**
     * NCLOB (Oracle)
     */
    public static final int NCLOB = 2011;

    /**
     * ROWID (Oracle)
     */
    public static final int ROWID = -8;

    /**
     * box/cidr/circle/inet/interval/line       (PostgreSQL)
     * lseg/macaddr/path/point/polygon/tsquery  (PostgreSQL)
     * tsvector/txid_snapshot/uuid/varbit/
     */
    public static final int POSTGRESQL_XTYPES = 1111;

    /**
     * xml    (PostgreSQL)
     */
    public static final int XML = 2009;

    /**
     * datetimeoffset          (SqlServer)
     */
    public static final int DATETIMEOFFSET = -155;
    public static final int SQLSERVER_SQL_VARIANT = -156;
    public static final int SQLSERVER_GEOMETRY = -157;
    public static final int SQLSERVER_GEOGRAPHY = -158;

    public static final int POSTGRESQL_COMPOSITE = 2002;

    /**
     * []*      (PostgreSQL)
     */
    public static final int POSTGRESQL_ARRAY = 2003;

    /**
     * bool (PostgreSQL)
     */
    public static final int X_POSTGRESQL_BOOL = -190;
    public static final String POSTGRESQL_BOOL = "bool";

    /**
     * bool (PostgreSQL)
     */
    public static final int X_POSTGRESQL_MONEY = -191;
    public static final String POSTGRESQL_MONEY = "money";

    /**
     * PG用户自定义类型 (PostgreSQL)
     */
    public static final int X_POSTGRESQL_CUSTOM_TYPE = 2002;

    /**
     * point/line/lseg/box/path/polygon/circle  (PostgreSQL)
     */
    public static final int POSTGRESQL_GEOMETRY = 1110;

    /**
     * SQLServer2005
     */
    public static final int SQL_SERVER_2005_TIMESTAMP = -151;
    public static final int SQL_SERVER_2005_SMALLDATETIME = -150;
    public static final int SQL_SERVER_2005_MONEY = -148;
    public static final int SQL_SERVER_2005_SMALLMONEY = -146;

    /**
     * DECFLOAT   (DB2)
     */
    public static final int DECFLOAT = 1111;

    public static final int X_OBJECT = -2019;
}
