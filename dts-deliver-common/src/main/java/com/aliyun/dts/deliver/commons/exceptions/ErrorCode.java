package com.aliyun.dts.deliver.commons.exceptions;

public interface ErrorCode {

    int SUCCESS = 0;

    /**
     * 不合理的参数，如下标超过范围，字符串为空
     */
    int COMMON_LIB_INVALID_PARAMETERS = 11004;

    /**
     * IO错误，如读写文件异常
     */
    int COMMON_LIB_IO_EXCEPTION = 11010;

    /**
     * 不合理的状态，如重复注册信号,30008
     */
    int FRAMEWORK_ILLEGAL_STATE = 31017; // illegal state

    /**
     * 从source读取数据异常,30010
     */
    int FRAMEWORK_READ_SOURCE_DATA = 31009;

    /**
     * the times retried exceeds the max retry times
     */
    int COMMON_LIB_EXCEED_MAX_RETRY_TIMES = 100047;

    /**
     * DB名映射失败
     */
    int COMMON_LIB_CREATE_DB_NAMES_MAPPER_ERROR = 10026;

    /**
     * 不支持的操作，如不支持某些数据类型,10020
     */
    int COMMON_LIB_UNSUPPORTED_OPERATION = 11005;

    /**
     * 不支持的record type
     */
    int CAPTURE_UNSUPPORTED_RECORD_TYPE = 50005;

    int REPLICATE_GET_TARGET_TABLE_META = 70002; // get table meta info from target database failed
    /**
     * 执行事务失败
     */
    int REPLICATE_EXECUTE_TRANSACTION_FAILED = 70003; // execute transaction on target db failed
    /**
     * 执行单个sql失败
     */
    int REPLICATE_EXECUTE_STATEMENT_FAILED = 70004; // execute single statement on target db failed
    /**
     * 提交事务失败
     */
    int REPLICATE_COMMIT_TRANSACTION_FAILED = 70005; // commit transaction on target db failed
    /**
     * 不支持的类型
     */
    int REPLICATE_UNSUPPORTED_OPERATION_TYPE = 70006; // unsupported operation type
    /**
     * 解析记录失败
     */
    int REPLICATE_RESOLVE_OPERATION_TYPE_FAILED = 70007; // resolve record operation failed
    /**
     * 释放事务失败
     */
    int REPLICATE_RELEASE_TRANSACTION_FAILED = 70008; // release transaction failed

    /**
     * 释放指定记录失败
     */
    int REPLICATE_RELEASE_RECORD_FAILED = 70021; // release specified record failed

    /**
     * 不容易分类的错误，未来慢慢细化
     */
    int FRAMEWORK_UNEXPECTED_ERROR = 31014;
}
