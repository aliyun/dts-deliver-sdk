package com.aliyun.dts.deliver.store;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.store.memory.MemoryThrottle;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.Total;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class AbstractRecordStoreWithMetrics {

    private final Sensor recordStoreInCountSensor;
    private final Sensor recordStoreOutCountSensor;
    private final Sensor recordStoreInByteSensor;
    private final Sensor recordStoreOutByteSensor;
    protected final Metrics metrics;
    protected MemoryThrottle memoryThrottle;
    protected DtsContext context;

    public AbstractRecordStoreWithMetrics(@Nonnull Metrics metrics, @Nonnull DtsContext context) {
        this.metrics = metrics;
        this.recordStoreInCountSensor = metrics.sensor("record-store-in-row");
        this.recordStoreInCountSensor.add(metrics.metricName("inCounts", "recordstore"), new Total());
        this.recordStoreInCountSensor.add(metrics.metricName("sourceRps", "recordstore"), new SimpleRate());
        this.recordStoreOutCountSensor = metrics.sensor("record-store-out-row");
        this.recordStoreOutCountSensor.add(metrics.metricName("outCounts", "recordstore"), new Total());
        this.recordStoreOutCountSensor.add(metrics.metricName("targetRps", "recordstore"), new SimpleRate());
        this.recordStoreInByteSensor = metrics.sensor("record-store-in-byte");
        this.recordStoreInByteSensor.add(metrics.metricName("inByte", "recordstore"), new Total());
        this.recordStoreInByteSensor.add(metrics.metricName("sourceBps", "recordstore"), new SimpleRate());
        this.recordStoreOutByteSensor = metrics.sensor("record-store-out-byte");
        this.recordStoreOutByteSensor.add(metrics.metricName("outByte", "recordstore"), new Total());
        this.recordStoreOutByteSensor.add(metrics.metricName("targetBps", "recordstore"), new SimpleRate());

        this.memoryThrottle = MemoryThrottle.getInstance();
        this.memoryThrottle.setMetrics(metrics);

        this.context = context;
    }

    public void publish(DtsMessage dtsMessage) {
        realPublish(dtsMessage);
        if (dtsMessage != null) {
            recordStoreInCountSensor.record(1);
        }
    }

    protected abstract void realPublish(DtsMessage dtsMessage);

    public Pair<RecordGroupCloseCallBack, List<Record>> consume(int supposedSize) {
        Pair<RecordGroupCloseCallBack, List<Record>> ret = realConsume(supposedSize);
        if (ret != null) {
            handleConsume(ret.getRight());
        }
        return ret;
    }

    protected abstract Pair<RecordGroupCloseCallBack, List<Record>> realConsume(int supposedSize);

    private void handleConsume(List<Record> records) {
        if (records != null) {
            long time = System.currentTimeMillis();
            records.forEach(record -> recordStoreOutByteSensor.record(record.size(), time));
            recordStoreOutCountSensor.record(records.size(), time);
        }
    }

    public abstract List<Pair<String, RecordCheckpoint>> checkpoint();

    public abstract boolean isEOF();

    public abstract void stop();
}
