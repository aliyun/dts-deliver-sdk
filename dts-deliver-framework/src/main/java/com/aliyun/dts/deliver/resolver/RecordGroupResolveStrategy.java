package com.aliyun.dts.deliver.resolver;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.framework.dispatcher.ToExecuteRecordBatchQueue;
import com.aliyun.dts.deliver.framework.dispatcher.record.RecordGroup;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;

import java.util.List;

public interface RecordGroupResolveStrategy {

    /**
     * Initialize strategy with @settings and output conflict-resolved transaction to @resolvedTransactions.
     */
    void initialize(Settings settings, ToExecuteRecordBatchQueue toExecutedQueue, Metrics metrics, Time time, DtsContext context);

    /**
     * Resolve traceableRecord, such as d dl dml .
     */
    boolean resolve(DtsMessage dtsMessage);

    /**
     * Release the recordGroups that are already consumed.
     *
     * @param recordGroups
     */
    void release(List<RecordGroup> recordGroups);

    List<Pair<String, RecordCheckpoint>> getSafeCheckpoint();

    void start();

    void stop();
}
