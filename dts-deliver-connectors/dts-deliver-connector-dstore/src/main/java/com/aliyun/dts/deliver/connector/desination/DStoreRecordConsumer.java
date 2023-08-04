package com.aliyun.dts.deliver.connector.desination;

import com.aliyun.dts.deliver.base.FailureTrackingDtsMessageConsumer;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.commons.exceptions.RecoverableDtsException;
import com.aliyun.dts.deliver.commons.functional.FunctionVoid;
import com.aliyun.dts.deliver.connector.desination.internal.DStoreRecordBuilder;
import com.aliyun.dts.deliver.connector.desination.internal.ProducerRecordWrapper;
import com.aliyun.dts.deliver.framework.processor.FutureRecords;
import com.aliyun.dts.deliver.framework.processor.InflightRecord;
import com.aliyun.dts.deliver.protocol.generated.ConfiguredDtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.taobao.drc.togo.client.producer.SchemafulProducerRecord;
import com.taobao.drc.togo.client.producer.TogoProducer;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaBuilder;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.util.concurrent.TogoCallback;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.taobao.drc.togo.data.schema.SchemaBuilder.int64;
import static com.taobao.drc.togo.data.schema.SchemaBuilder.string;

public class DStoreRecordConsumer extends FailureTrackingDtsMessageConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DStoreRecordConsumer.class);

    protected String name = "dstore";

    private DStoreDestinationConfig dStoreDestinationConfig;
    private TogoProducer producer;
    private ConfiguredDtsCatalog catalog;
    private Consumer<DtsMessage> outputRecordCollector;

    private DStoreRecordBuilder dStoreRecordBuilder;

    protected String topic;

    private Schema schema;

    private Map<String, SchemaMetaData> schemaMetaDatas = new HashMap<>(2);

    private boolean needTimestamp;

    private Throwable lastException;

    protected Consumer<Throwable> errorHandler;

    private int produceBigSize;

    private int wattingTimeOut;

    private AtomicBoolean hasErrorOnSend = new AtomicBoolean(false);

    protected int recordRetryTime;

    private ExecutorService callBackExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, name);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOGGER.error("Uncaught exception in thread'" + t.getName() + "':", e);
                }
            });
            return thread;
        }
    });

    private int partitionNums;

    public DStoreRecordConsumer(Settings settings, DStoreDestinationConfig dStoreDestinationConfig) {
        this.dStoreDestinationConfig = dStoreDestinationConfig;
        this.producer = dStoreDestinationConfig.getProducer();
        this.topic = GlobalSettings.DTS_DELIVER_TOPIC.getValue(settings);

        this.catalog = catalog;
        this.outputRecordCollector = outputRecordCollector;

        this.dStoreRecordBuilder = new DStoreRecordBuilder(false);

        this.needTimestamp = true;

        this.schema = SchemaBuilder.struct().name("default")
                .field("dbName", string())
                .field("tbName", string())
                .field("threadID", int64())
                .field("extraTag", int64()) // current high 4 - 7 byte is hash value, 2- 3 byte is region id, 0 - 1 byte is record type
                .field("timestamp", int64())
                .field("extraIndex", int64())
                .build();

        this.produceBigSize = 2 * Integer.parseInt(dStoreDestinationConfig.getProperties().getProperty(ProducerConfig.BATCH_SIZE_CONFIG, "33554432"));

        this.wattingTimeOut = 2 * Integer.parseInt(dStoreDestinationConfig.getProperties().getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000"));

        errorHandler = (t) -> {
            hasErrorOnSend.set(true);
            lastException = t;
            LOGGER.error("DStoreSink: produce failed cause " + t.getMessage(), t);
        };

        this.recordRetryTime = GlobalSettings.RECORD_ERROR_RETRY_TIME_SETTING.getValue(settings);

        partitionNums = GlobalSettings.DTS_DELIVER_TOPIC_PARTITION_NUM.getValue(settings);
    }

    @Override
    protected void startTracked() throws Exception {
    }

    @Override
    protected Future<Void> acceptTracked(List<Record> records) throws Exception {
        FutureRecords<Void> futureRecords = new FutureRecords<>(records);

        executeDMLSQLRecordsWithRetry(futureRecords);

        return futureRecords;
    }

    // visible for test
    public long executeDMLSQLRecordsWithRetry(FutureRecords<Void> futureRecords) {
        for (int i = 0; i < recordRetryTime; ++i) {
            try {
                return executeDMLSQLByRecords(futureRecords);
            } catch (RecoverableDtsException e) {
                LOGGER.info(name + " retry at [" + (i + 1) + "] time,  cause " + e.getMessage(), e);
                continue;
            }
        }
        throw new CriticalDtsException("nosql-replicate", ErrorCode.REPLICATE_EXECUTE_STATEMENT_FAILED,
                name + " execute " + StringUtils.join(futureRecords, ",") + "has excess max record retry time [" + recordRetryTime + "]");
    }

    private long executeDMLSQLByRecords(FutureRecords<Void> futureRecords) {
        long affectCount = 0;
        for (final InflightRecord inflightRecord : futureRecords.getInFlightRequests()) {

            try {
                write(futureRecords, inflightRecord);
            } catch (Exception e) {
                LOGGER.error(name + " execute record failed cause " + e.getMessage() + ", record is: " + inflightRecord, e);
                errorHandler.accept(e);
            }
        }
        return affectCount;
    }

    public void write(FutureRecords<Void> futureRecords, InflightRecord record) throws Exception {

        mayThrowException();

        SchemafulProducerRecord schemafulProducerRecord = null;
        try {

            int partition = calcutePartition(partitionNums, record.getGroupKey());

            schemafulProducerRecord = this.dStoreRecordBuilder.build(topic, partition, this.schema, getSchemaMetadata(topic), record, needTimestamp);
        } catch (ExecutionException e) {
            LOGGER.error("Error on Serialize Record: " + record.getSourcePosition());
            throw new RuntimeException("Cannot Serialize Record.", e);
        }

        ProducerRecordWrapper producerRecordWrapper = new ProducerRecordWrapper(schemafulProducerRecord, topic, record.getSourcePosition(), getTimestampMs(record));

        producerRecordWrapper.beginProduce();

        realReplicateRecord(producerRecordWrapper, () -> futureRecords.complete(record));
    }

    private int calcutePartition(int partitionNums, long groupKey) {
        return (int) (groupKey % partitionNums);
    }

    private void mayThrowException() {
        if (this.hasErrorOnSend.get()) {
            throw new RuntimeException("Error on Send." + lastException.getMessage(), lastException);
        }
    }


    private SchemaMetaData getSchemaMetadata(String topicName) throws ExecutionException, InterruptedException {
        if (!this.schemaMetaDatas.containsKey(topicName)) {
            TogoFuture<SchemaMetaData> future = producer.registerSchema(topicName, "default", schema);
            this.schemaMetaDatas.put(topicName, future.get());
        }
        return this.schemaMetaDatas.get(topicName);
    }

    private long getTimestampMs(Record record) {
        if (record.getSourceTypeAndVersion() != null
                && record.getSourceTypeAndVersion().getLeft() != null) {
            switch (record.getSourceTypeAndVersion().getLeft().toLowerCase()) {
                case "postgresql":
                case "redis":
                    return record.getTimestamp();
                default:
                    return record.getTimestamp() * 1000;
            }
        }
        return record.getTimestamp() * 1000;
    }

    public int realReplicateRecord(ProducerRecordWrapper producerRecord, FunctionVoid checkpointFunctionVoid)
            throws InterruptedException, ExecutionException, TimeoutException {

        TogoFuture<RecordMetadata> future = producer.send(producerRecord.getSchemafulProducerRecord());

        if (producerRecord.getSchemafulProducerRecord().data().length > this.produceBigSize) {
            RecordMetadata metadata = null;
            try {
                metadata = future.get(this.wattingTimeOut, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException exception) {
                LOGGER.error("Exception on TProducer.", exception);
                LOGGER.error("RecordMetadata: " + metadata);
                errorHandler.accept(exception);
                close(true);
                throw exception;
            }
        } else {
            future.addCallback(new TogoCallback<RecordMetadata>() {
                @Override
                public void onComplete(RecordMetadata metadata, Throwable exception) {
                    if (exception != null) {
                        if (!hasErrorOnSend.get()) {
                            LOGGER.error("Exception on TProducer.", exception);
                            LOGGER.error("RecordMetadata: " + metadata);
                            errorHandler.accept(exception);
                            producer.close(0, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        checkpointFunctionVoid.call();
                        metric(producerRecord);
                    }
                }
            }, callBackExecutor);
        }

        return 1;
    }
//
//    public void close() {
//        if (this.producer != null) {
//            this.producer.close(0, TimeUnit.MILLISECONDS); // 马上关闭
//            this.producer = null;
//        }
//    }

    @Override
    protected void close(boolean hasFailed) {
        if (this.producer != null) {
            producer.flush();
            producer.close();
        }
    }

    //todo(yanmen)
    private void metric(ProducerRecordWrapper producerRecordWrapper) {
    }
}
