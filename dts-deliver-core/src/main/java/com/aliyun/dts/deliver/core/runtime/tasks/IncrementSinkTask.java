package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.DtsMessageConsumer;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.exceptions.DTSCommonException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.core.runtime.pipeline.RecordPipeline;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import com.aliyun.dts.deliver.store.RecordGroupCloseCallBack;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class IncrementSinkTask extends SinkTask {
    /**
     * Construct a SinkTask object to handle sink task.
     *
     * @param settings
     * @param destination
     * @param recordStore
     * @param destPipeline
     */

    //slide future list
    private Queue<Future> futureQueue = new ArrayDeque<>();
    private int MAX_FUTURE_LIST_SIZE = 10;

    public IncrementSinkTask(Settings settings, Destination destination, AbstractRecordStoreWithMetrics recordStore, RecordPipeline destPipeline) {
        super(settings, destination, recordStore, destPipeline);
    }

    @Override
    protected void sinkRecords(Destination destination, AbstractRecordStoreWithMetrics recordStore, RecordPipeline destPipeline, int batchSize) throws Exception {

        Pair<RecordGroupCloseCallBack, List<Record>> recordsPair =  recordStore.consume(batchSize);

        if (null == recordsPair) {
            if (recordStore.isEOF()) {
                // record store is EOF, and we can not get new data, just stop
                stop();
            }
            return;
        }

        inState("Write");

        RecordGroupCloseCallBack recordGroupCloseCallBack = recordsPair.getLeft();
        List<Record> records = recordsPair.getRight();

        List<Record> newRecords = RecordPipeline.cookRecords(records, destPipeline).getLeft();

        try {
            context.retry(
                    () -> {
                        try {

                            Future<Void> future = destination.accept(newRecords);

                            future.addListener((error) -> {
                                recordGroupCloseCallBack.onClose();
                            });
                            if (!futureQueue.add(future)) {
                                throw new DTSCommonException(ErrorCode.FRAMEWORK_UNEXPECTED_ERROR);
                            }

                            // try process futures in queue
                            while (futureQueue.size() > MAX_FUTURE_LIST_SIZE) {
                                future = futureQueue.peek();
                                future.get();
                                futureQueue.remove();
                            }
                            if (future.isDone()) {
                                future = futureQueue.peek();
                                while (null != future) {
                                    if (future.isDone()) {
                                        futureQueue.remove();
                                        future = futureQueue.peek();
                                    } else {
                                        break;
                                    }
                                }
                            }
                        } catch (Exception foo) {
                            /**
                             * error happened, try to close record range to release stale resources;
                             * otherwise, try to reuse previous resources
                             */
                            LOGGER.warn("Sink record failed, close record range, maybe try again", foo);
                            destination.close();
                            throw foo;
                        }
                    }, (e, t) -> destination.isRecoverable(e));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            SwallowException.callAndSwallowException(() -> destination.close());
            throw e;
        }
    }
}
