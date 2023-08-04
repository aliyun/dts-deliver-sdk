package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.core.runtime.pipeline.DtsMessagePipeline;
import com.aliyun.dts.deliver.core.runtime.pipeline.RecordPipeline;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

public abstract class SinkTask extends Task {

    public static final String BASE_TASK_NAME = "sink";
    private static final AtomicLong SINK_TASK_ID_GENERATOR = new AtomicLong(0);

    protected static final Logger LOGGER = LoggerFactory.getLogger(SinkTask.class);

    protected final Settings settings;

    private final Destination destination;
    private AbstractRecordStoreWithMetrics recordStore;

    protected final RecordPipeline destPipeline;

    /**
     * Construct a SinkTask object to handle sink task.
     */
    public SinkTask(Settings settings, Destination destination, AbstractRecordStoreWithMetrics recordStore,
                    RecordPipeline destPipeline) {

        super(BASE_TASK_NAME, SINK_TASK_ID_GENERATOR.incrementAndGet());

        this.settings = settings;

        this.destination = destination;
        this.recordStore = recordStore;

        this.destPipeline = destPipeline;

    }

    protected abstract void sinkRecords(Destination destination, AbstractRecordStoreWithMetrics recordStore,
                                        RecordPipeline destPipeline, int batchSize) throws Exception;

    @Override
    public final void safeRun() throws Exception {
        final long sinkerId = getId();

        destination.open(settings, sinkerId);

        while (!isStopped()) {
            int maxBatchSize = GlobalSettings.SINK_TASK_RECORD_MAX_BATCH_SIZE.getValue(settings);
            sinkRecords(destination, recordStore, destPipeline, maxBatchSize);
        }
    }
}
