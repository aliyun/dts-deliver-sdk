package com.aliyun.dts.deliver.core.runtime;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.RecordInterceptor;
import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.etl.ETLInstance;
import com.aliyun.dts.deliver.commons.util.Time;
import com.aliyun.dts.deliver.core.runtime.pipeline.DtsMessagePipeline;
import com.aliyun.dts.deliver.core.runtime.pipeline.RecordPipeline;
import com.aliyun.dts.deliver.core.runtime.tasks.Task;
import com.aliyun.dts.deliver.core.runtime.tasks.TaskSubmitter;
import com.aliyun.dts.deliver.framework.dispatcher.RecordAssemblyDispatcher;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DtsIntegrationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtsIntegrationRunner.class);

    private final Settings settings;

    private final ETLInstance etlInstance;

    private final TaskSubmitter taskSubmitter;

    private AbstractRecordStoreWithMetrics recordStore;

    private AtomicReference<Throwable> errorRef;

    private final Metrics metrics;

    private Destination destination;
    private List<Source> sourceList;
    private DtsContext context;

    private int topicPartitionNum;

    private final Consumer<List<Pair<String, RecordCheckpoint>>> checkpointConsumer;

    private ScheduledThreadPoolExecutor checkpointExecutor;
    ScheduledFuture checkpointScheduledFuture;

    private List<Task> soureTasks = new ArrayList<>();
    private List<Task> sinkTasks = new ArrayList<>();

    public DtsIntegrationRunner(Settings settings, Metrics metrics, Destination destination, List<Source> sourceList, DtsContext context, Consumer<List<Pair<String, RecordCheckpoint>>> checkpointConsumer) {
        this.settings = settings;
        this.metrics = metrics;
        this.etlInstance = new ETLInstance(settings);
        this.destination = destination;
        this.sourceList = sourceList;
        this.context = context;

        this.topicPartitionNum = GlobalSettings.DTS_DELIVER_TOPIC_PARTITION_NUM.getValue(settings);

        taskSubmitter = new TaskSubmitter(etlInstance, settings, metrics, context);

        this.recordStore = taskSubmitter.getRecordStore().orElseGet(() -> new RecordAssemblyDispatcher(metrics, context, settings));

        this.checkpointConsumer = checkpointConsumer;

    }

    private DtsMessagePipeline buildSourceMessagePipeline(Source source) {
        DtsMessagePipeline rs = null;

        List<DtsMessageInterceptor> interceptors = source.recordInterceptors(metrics);

        if (!interceptors.isEmpty()) {
            rs = new DtsMessagePipeline(interceptors);
        }

        return rs;
    }

    private RecordPipeline buildDestinationMessagePipeline(Destination destination) {
        RecordPipeline rs = null;

        List<RecordInterceptor> interceptors = destination.recordInterceptors(metrics);

        if (!interceptors.isEmpty()) {
            rs = new RecordPipeline(interceptors);
        }

        return rs;
    }

    public void start() {

        LOGGER.info("start source and dest task");
        for(Source source : sourceList) {
            //add unique name
            context.addSource(source.uniqueName());
            context.addStartCheckpoints(source.uniqueName(), source.startCheckpoint());
            DtsMessagePipeline sourceMessagePipeline = buildSourceMessagePipeline(source);
            Task sourceTask = taskSubmitter.submitSourceTask(recordStore, source, sourceMessagePipeline);

            soureTasks.add(sourceTask);
        }

        RecordPipeline destinationRecordPipeline = buildDestinationMessagePipeline(destination);
        Task sinkTask = taskSubmitter.submitSinkTask(recordStore, destination, destinationRecordPipeline);
        sinkTasks.add(sinkTask);

        saveCheckpointPeriodically();
    }

    private void saveCheckpointPeriodically() {
        LOGGER.info("save Checkpoint Periodically");

        checkpointExecutor = new ScheduledThreadPoolExecutor(1);
        checkpointScheduledFuture = checkpointExecutor.scheduleAtFixedRate(
                () -> {
                    List<Pair<String, RecordCheckpoint>> checkpoints = recordStore.checkpoint();
                    checkpointConsumer.accept(checkpoints);
                }, 1,
                3,
                TimeUnit.SECONDS);

    }

    public void stop() {
        LOGGER.info("stop context");
        context.stop();

        //stop time
        LOGGER.info("stop time");
        Time.stop();

        //checkpoint
        LOGGER.info("stop checkpoint");
        checkpointExecutor.shutdownNow();
        checkpointScheduledFuture.cancel(true);

        //tasks
        LOGGER.info("stop soure and sink tasks");
        taskSubmitter.stop();
        soureTasks.forEach(sourceTask -> sourceTask.stop());
        sinkTasks.forEach(sinkTask -> sinkTask.stop());

        //store
        LOGGER.info("stop store");
        recordStore.stop();
    }
}
