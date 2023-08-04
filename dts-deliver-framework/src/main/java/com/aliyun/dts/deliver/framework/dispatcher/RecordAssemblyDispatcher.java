package com.aliyun.dts.deliver.framework.dispatcher;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.framework.dispatcher.record.RecordGroup;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.resolver.DefaultRecordGrouResolveStrategy;
import com.aliyun.dts.deliver.resolver.RecordGroupResolveStrategy;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import com.aliyun.dts.deliver.store.RecordGroupCloseCallBack;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordAssemblyDispatcher extends AbstractRecordStoreWithMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(RecordAssemblyDispatcher.class);

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private Settings settings;

    @VisibleForTesting
    protected RecordGroupResolveStrategy resolveStrategy = null;

    @VisibleForTesting
    private ToExecuteRecordBatchQueue independentRecordQueue = null;

    private final Time time;

    private String transId;

    private long transSeq;

    public RecordAssemblyDispatcher(@Nonnull Metrics metrics, DtsContext context, Settings settings) {
        super(metrics, context);
        this.time = Time.SYSTEM;
        this.context = context;

        initialize(settings);
    }

    public void initialize(Settings settings) {
        if (initialized.compareAndSet(false, true)) {

            this.settings = settings;

            this.resolveStrategy = new DefaultRecordGrouResolveStrategy();

            this.independentRecordQueue = new ToExecuteRecordBatchQueue(settings, metrics);

            this.resolveStrategy.initialize(this.settings, this.independentRecordQueue, metrics,
                    time, this.context);

            this.resolveStrategy.start();

            LOG.info("RecordGroupResolveStrategy: init success");
        } else {
            LOG.info("RecordGroupResolveStrategy: has initialized, ignore");
        }
    }

    @Override
    public boolean isEOF() {
        return false;
    }

    @Override
    public void stop() {
        this.resolveStrategy.stop();
    }

    @Override
    public void realPublish(DtsMessage dtsMessage) {
        if (dtsMessage.getRecord() != null) {
            String sourceName = dtsMessage.getRecord().getUniqueSourceName();
            Preconditions.checkArgument(this.resolveStrategy.resolve(dtsMessage));
        }
    }

    @Override
    public Pair<RecordGroupCloseCallBack, List<Record>> realConsume(int supposedSize) {

        List<RecordGroup> recordGroups = independentRecordQueue.pollRecordBatch(supposedSize);

        if (recordGroups == null) {
            return null;
        }

        return Pair.of(() -> resolveStrategy.release(recordGroups),
                flatRecordGroupToRecordList(recordGroups));
    }

    protected List<Record> flatRecordGroupToRecordList(List<RecordGroup> recordGroups) {
        List<Record> records = new ArrayList<>(recordGroups.size());
        for (RecordGroup recordGroup : recordGroups) {
            for (Record record : recordGroup.getRecordList()) {
                records.add(record);
            }
        }
        return records;
    }

    synchronized long computeTransSeq(Record record) {

        if (this.transId == null) {
            this.transId = record.getTransactionId();
        }

        if (record.getTransactionId() != null) {
            if (record.getTransactionId().equals(this.transId)) {
                this.transSeq++;
            } else {
                this.transSeq = 1;
                this.transId = record.getTransactionId();
            }
        } else {
            LOG.error("Record " + record.toString() + "does not has transaction");
            return -1;
        }
        return this.transSeq;
    }

    @Override
    public List<Pair<String, RecordCheckpoint>> checkpoint() {
        return resolveStrategy.getSafeCheckpoint();
    }
}
