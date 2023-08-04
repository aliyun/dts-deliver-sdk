package com.aliyun.dts.deliver.base;


import com.aliyun.dts.deliver.commons.functional.CheckedConsumer;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;

import java.util.List;

/**
 * Interface for the destination's consumption of incoming records wrapped in an
 *
 * This is via the accept method, which commonly handles parsing, validation, batching and writing
 * of the transformed data to the final destination i.e. the technical system data is being written
 * to.
 *
 * Lifecycle:
 * <ul>
 * <li>1. Instantiate consumer.</li>
 * <li>2. start() to initialize any resources that need to be created BEFORE the consumer consumes
 * any messages.</li>
 * <li>3. Consumes ALL records via {@link DtsMessageConsumer#accept(List<DtsMessage>)}</li>
 * <li>4. Always (on success or failure) finalize by calling
 * {@link DtsMessageConsumer#close()}</li>
 * </ul>
 * We encourage implementing this interface using the {@link FailureTrackingDtsMessageConsumer}
 * class.
 */
public interface DtsMessageConsumer extends CheckedConsumer<List<Record>, Exception>, AutoCloseable {

    void start() throws Exception;

    /**
     * Consumes all {@link DtsMessage}s
     *
     * @param records {@link DtsMessage} to be processed
     * @throws Exception
     */
    @Override
    Future<Void> accept(List<Record> records) throws Exception;

    /**
     * Executes at the end of consumption of all incoming streamed data regardless of success or failure
     *
     * @throws Exception
     */
    @Override
    void close() throws Exception;

    /**
     * Append a function to be called on {@link DtsMessageConsumer#close}.
     */
//    static DtsMessageConsumer appendOnClose(final DtsMessageConsumer consumer, final VoidCallable voidCallable) {
//        return new DtsMessageConsumer() {
//
//            @Override
//            public void start() throws Exception {
//                consumer.start();
//            }
//
//            @Override
//            public void accept(final DtsMessage message) throws Exception {
//                consumer.accept(message);
//            }
//
//            @Override
//            public void close() throws Exception {
//                consumer.close();
//                voidCallable.call();
//            }
//
//        };
//    }

}