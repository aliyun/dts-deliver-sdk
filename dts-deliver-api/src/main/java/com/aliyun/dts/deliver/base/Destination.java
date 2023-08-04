package com.aliyun.dts.deliver.base;

import com.aliyun.dts.deliver.RecordInterceptor;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;

import java.util.Collections;
import java.util.List;

import com.aliyun.dts.deliver.commons.json.Jsons;
import com.aliyun.dts.deliver.protocol.record.Record;
import org.apache.kafka.common.metrics.Metrics;

public interface Destination extends Integration {

    /**
     * Return a consumer that writes messages to the destination.
     *
     * @return Consumer that accepts message.
     *         will be called n times where n is the number of messages.
     *         {@link DtsMessageConsumer#close()} will always be called once regardless of success
     *         or failure.
     * @throws Exception - any exception.
     * @param sinkId
     */
    DtsMessageConsumer getConsumer(long sinkId)
            throws Exception;

    static void defaultOutputRecordCollector(final DtsMessage message) {
        System.out.println(Jsons.serialize(message));
    }

    Boolean isRecoverable(Throwable e);

    /**
     * Open the destination for further put operation
     *
     */
    void open(Settings settings, long sinkerId) throws Exception;


    /**
     * Get the record interceptors used to intercept records before putting in record-store, or intercept records
     * after polling from record-store.
     * If current processor works as Source mode, the interceptors should be inserted before record store; otherwise,
     * that should be inserted after record store.
     * @return record filters
     */
    default List<RecordInterceptor> recordInterceptors(Metrics metrics) {
        return Collections.emptyList();
    }

    /**
     * Consumes all {@link DtsMessage}s
     *
     * @param records {@link DtsMessage} to be processed
     * @throws Exception
     */
    Future<Void> accept(List<Record> records) throws Exception;
}
