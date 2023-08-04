package com.aliyun.dts.deliver.base;

import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.RecordInterceptor;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.util.AutoCloseableIterator;
import com.aliyun.dts.deliver.protocol.generated.ConfiguredDtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.DtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.metrics.Metrics;

import java.util.Collections;
import java.util.List;

public interface Source extends Integration {

    /**
     * Discover the current schema in the source.
     *
     * @param config - integration-specific configuration object as json. e.g. { "username": "Dts",
     *        "password": "super secure" }
     * @return Description of the schema.
     * @throws Exception - any exception.
     */
    DtsCatalog discover(JsonNode config) throws Exception;

    /**
     * Return a iterator of messages pulled from the source.
     *
     * @param config - integration-specific configuration object as json. e.g. { "username": "Dts",
     *        "password": "super secure" }
     * @param catalog - schema of the incoming messages.
     * @param state - state of the incoming messages.
     * @return {@link AutoCloseableIterator} that produces message. The iterator will be consumed until
     *         no records remain or until an exception is thrown. {@link AutoCloseableIterator#close()}
     *         will always be called once regardless of success or failure.
     * @throws Exception - any exception.
     */
    AutoCloseableIterator<DtsMessage> read(Settings config, ConfiguredDtsCatalog catalog, JsonNode state) throws Exception;

    /**
     * Open the source, do some prepare operation, such as find the break checkpoint to continue.
     *
     */
    void open() throws Exception;

    /**
     * Determine if this exception happened in AnySource can be recovered. If so, the framework will retry the failed
     * routine.
     * @param throwable the exception to check
     * @return true if current exception can be retried
     */
    default boolean isRecoverable(Throwable throwable) {
        return false;
    }

    /**
     *
     * @return unique name
     */
    String uniqueName();

    /*
    * return start checkpoint
     */
    RecordCheckpoint startCheckpoint();

    /**
     * Get the record interceptors used to intercept records before putting in record-store, or intercept records
     * after polling from record-store.
     * If current processor works as Source mode, the interceptors should be inserted before record store; otherwise,
     * that should be inserted after record store.
     * @return record filters
     */
    default List<DtsMessageInterceptor> recordInterceptors(Metrics metrics) {
        return Collections.emptyList();
    }
}
