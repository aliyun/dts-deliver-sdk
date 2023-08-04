package com.aliyun.dts.deliver.commons.config;

public interface GlobalSettings {
    Settings.Setting<Integer> MAX_RETRY_SECONDS = Settings.integerSetting(
            "maxRetryTime", "max retry seconds for source/sink functions", 12 * 3600);
    Settings.Setting<Integer> RETRY_BLIND_SECONDS = Settings.integerSetting(
            "retry.blind.seconds", "blind retry seconds, this value must smaller than maxRetryTime", 10 * 60);
    Settings.Setting<Integer> RETRY_SLEEP_SECONDS = Settings.integerSetting(
            "retry.sleep.seconds", "sleep for some seconds before next retry for source/sink functions", 10);

    Settings.Setting<Integer> SINK_TASK_RECORD_MAX_BATCH_SIZE = Settings.integerSetting(
            "dts.datamove.record.batch.size.max", "the bath size for sink task to call put", 1024);

    Settings.Setting<String> DTS_OPENAPI_REGION = Settings.stringSetting(
            "dts.openapi.region", "dts openapi region");

    Settings.Setting<String> DTS_OPENAPI_ENDPOINT = Settings.stringSetting(
            "dts.openapi.endpoint", "dts openapi endpoint");

    Settings.Setting<String> ALIYUN_AK = Settings.stringSetting(
            "aliyun.ak", "the aliyun ak to get real user password and dbList");
    Settings.Setting<String> ALIYUN_SECRET = Settings.stringSetting(
            "aliyun.secret", "the aliyun secret to get real user password and dbList");

    Settings.Setting<String> DTS_DELIVER_USER = Settings.stringSetting(
            "dts.deliver.user", "the dts deliver user name");
    Settings.Setting<String> DTS_DELIVER_PASSWORD = Settings.stringSetting(
            "dts.deliver.password", "the dts deliver password");

    Settings.Setting<String> DTS_JOB_ID = Settings.stringSetting(
            "dts.job.id", "dts job id");

    Settings.Setting<String> DTS_DELIVER_TOPIC = Settings.stringSetting(
            "dts.deliver.topic", "dts deliver topic");

    Settings.Setting<String> DTS_BOOTSTRAP_SERVERS_CONFIG = Settings.stringSetting(
            "dts.bootstrap.servers", "dts deliver url, usually dproxy");

    Settings.Setting<Integer> DTS_DELIVER_TOPIC_PARTITION_NUM = Settings.integerSetting(
            "dts.deliver.topic.partition.num", "dts deliver topic partition num", 0);

    Settings.Setting<String> DB_MAPPER_JSON_EXPRESSIONS =
            Settings.stringSetting("dts.datamove.mapper.expressions",
                    "The expressions used to mapper db, table and columen", "");

    // transaction framework related configs
    Settings.Setting<Integer> TRANS_EXECUTE_QUEUE_SIZE = Settings.integerSetting(
            "trans.execute.queue.size", "the max queue size to store record batch to be executed", Integer.MAX_VALUE);


    Settings.Setting<Integer> RECORD_GROUP_ASSEMBLY_SIZE_LIMIT = Settings.integerSetting(
            "record.group.assembly.size.limit", "The assembly size records group", 32);

    //replicate retry time, for communication failure eg
    Settings.Setting<Integer> RECORD_ERROR_RETRY_TIME_SETTING =
            Settings.integerSetting("recordReplicateErrorRetryTime",
                    "record replicate error retry time, this used to be reconnect action",
                    150);

    Settings.Setting<String> GLOBAL_JOB_TYPE = Settings.stringSetting(
            "global.job.type", "the ", "unknown");

    Settings.Setting<String> KAFKA_SECURITY_PROTOCOL = Settings.stringSetting(
            "kafka.security.protocol", "DStore kafka security protocal", "SASL_PLAINTEXT");
    Settings.Setting<String> KAFKA_SASL_MECHANISM = Settings.stringSetting(
            "kafka.sasl.mechanism", "DStore kafka sasl mechanism", "PLAIN");
}
