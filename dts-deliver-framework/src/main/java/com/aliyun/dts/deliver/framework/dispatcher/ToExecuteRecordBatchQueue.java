package com.aliyun.dts.deliver.framework.dispatcher;

import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.framework.dispatcher.record.RecordGroup;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatchType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ToExecuteRecordBatchQueue {
    public static final Settings.Setting<Integer> POLL_TIMEOUT_SECONDS = Settings.integerSetting("batch.queue.poll.timeout.seconds",
        "The seconds to be waited for polling the batch queue", 10);

    private static final Logger LOG = LoggerFactory.getLogger(ToExecuteRecordBatchQueue.class);

    private final LinkedBlockingQueue<RecordGroup> recordBatchQueue;
    private final Metrics metrics;
    private final int pollTimeoutSeconds;

    public ToExecuteRecordBatchQueue(Settings settings, Metrics metrics) {
        //Notice!! the blocking queue size should greater than syncConflictCapacity in BucketResolve otherwise deadlock may happened
        //Config it manually is not recommended
        int queueSize = GlobalSettings.TRANS_EXECUTE_QUEUE_SIZE.getValue(settings);
        recordBatchQueue = new LinkedBlockingQueue<>(queueSize);
        pollTimeoutSeconds = POLL_TIMEOUT_SECONDS.getValue(settings);

        this.metrics = metrics;
        LOG.info("ToExecuteTransactionQueue: init execute queue size [{}]", queueSize);
    }

    public boolean in(RecordGroup recordBatch) {
        try {
            while (!recordBatchQueue.offer(recordBatch, 10000, TimeUnit.MILLISECONDS)) {
                LOG.info("ToExecuteTransactionQueue: offer transaction [{}] has failed for 10 seconds, may replicate sink blocked", recordBatch);
            }
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    public int holdingCount() {
        return recordBatchQueue.size();
    }

    public List<RecordGroup> pollRecordBatch(int supposedRecordSizes) {
        final Map<String, Long> tableVersionMap = new HashMap<>(64);
        RecordGroup recordBatch;

        try {
            recordBatch = recordBatchQueue.poll(pollTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }

        if (null == recordBatch) {
            return null;
        }

        List<RecordGroup> ret = new LinkedList<>();
        ret.add(recordBatch);

        RecordBatchType expectedBatchType = recordBatch.getRecordBatchType();
        if (RecordBatchType.ddl == expectedBatchType) {
            return ret;
        }

        // try to poll more trans for per sink worker
        int currentRecordCount = recordBatch.getRecordCount();
        while (currentRecordCount < supposedRecordSizes) {
            final RecordGroup moreRecordBatch = recordBatchQueue.poll();
            if (null == moreRecordBatch) {
                break;
            }
            if (moreRecordBatch.getRecordBatchType() != expectedBatchType) {
                SwallowException.callAndThrowRuntimeException(() -> recordBatchQueue.put(moreRecordBatch));
                break;
            }
            ret.add(moreRecordBatch);
            currentRecordCount += moreRecordBatch.getRecordCount();
        }

        return ret;
    }

    public boolean isEmpty() {
        return recordBatchQueue.isEmpty();
    }
}
