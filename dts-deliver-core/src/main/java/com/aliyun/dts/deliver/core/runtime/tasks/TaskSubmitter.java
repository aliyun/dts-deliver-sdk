package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.etl.ETLInstance;
import com.aliyun.dts.deliver.commons.thread.ThreadFactoryWithNamePrefix;
import com.aliyun.dts.deliver.core.runtime.DtsIntegrationRunner;
import com.aliyun.dts.deliver.core.runtime.pipeline.DtsMessagePipeline;
import com.aliyun.dts.deliver.core.runtime.pipeline.RecordPipeline;
import com.aliyun.dts.deliver.core.runtime.standalone.StandaloneContext;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskSubmitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSubmitter.class);

    private final Settings settings;
    private ThreadPoolExecutor criticalTaskThreadPool;
    private ThreadPoolExecutor normalTaskThreadPool;

    private TaskManager<SourceTask> sourceTaskManager;
    private TaskManager<SinkTask> sinkTaskManager;

    private final ETLInstance etlInstance;

    private final Metrics metrics;

    private DtsContext context;

    public TaskSubmitter(ETLInstance etlInstance, Settings settings, Metrics metrics, DtsContext context) {
        this.etlInstance = etlInstance;
        this.settings = settings;
        this.metrics = metrics;
        this.context = context;

        this.normalTaskThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
                TimeUnit.SECONDS, new SynchronousQueue<>(), new ThreadFactoryWithNamePrefix("any-all-normalTaskThreadPool-"));
    }

    //todo(yanmen)
    public Optional<AbstractRecordStoreWithMetrics> getRecordStore() {
        return Optional.empty();
    }


    public Task submitNormalTask(Task task) {
        LOGGER.info("sublit normal task: " + task);
        normalTaskThreadPool.submit(task);

        return task;
    }

    public Task submitSourceTask(AbstractRecordStoreWithMetrics recordStore, Source source, DtsMessagePipeline messagePipeline) {
        SourceTask task = new SourceTask(this.settings, source, recordStore, messagePipeline);
        task.setContext(context);
        return submitNormalTask(task);
    }

    public Task submitSinkTask(AbstractRecordStoreWithMetrics recordStore, Destination destination, RecordPipeline recordPipeline) {
        SinkTask task = new IncrementSinkTask(this.settings, destination, recordStore, recordPipeline);
        task.setContext(context);
        return submitNormalTask(task);
    }

    public void stop() {
        normalTaskThreadPool.shutdownNow();
    }
}
