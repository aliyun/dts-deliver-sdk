package com.aliyun.dts.deliver.store.memory;

import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.collections4.CollectionUtils;

public class MemoryThrottle {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryThrottle.class);

    private long maxWriterMemory;
    private static final long SWELL_FACTOR = 3;

    private AtomicLong usedBytes = new AtomicLong(0);

    //default max memory 2G
    private static final long MAX_MEMORY = 2 * 1024;
    private static final long BYTES_PER_MB = 1024 * 1024;
    private volatile boolean memoryBlock = false;
    private long maxAllowMemory;

    //MB unit
    public static final Settings.Setting<Long> MAX_WRITER_MEMORY = Settings.longSetting(
            "any.memory.store.max.mb", "store max mb size", MAX_MEMORY);

    private Settings settings;

    private static MemoryThrottle instance;

    private Metrics metrics;

    public static MemoryThrottle getInstance() {
        if (instance == null) {
            synchronized (MemoryThrottle.class) {
                if (instance == null) {
                    instance = new MemoryThrottle();
                }
            }
        }
        return instance;
    }

    public MemoryThrottle() {
        MemoryMXBean mxb = ManagementFactory.getMemoryMXBean();
        this.maxAllowMemory = mxb.getHeapMemoryUsage().getMax() / SWELL_FACTOR;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
        this.metrics.addMetric(metrics.metricName("usedBytes", "MemoryThrottle"), (config, now) -> usedBytes.get());
    }

    public long beforePublishRecords(DtsMessage dtsMessage) {
        long size = calculatorSize(dtsMessage);
        waitUntilMemoryAvailable(size);
        return size;
    }

    public void afterPublishRecords(long size) {
        addUsedBytes(size * SWELL_FACTOR);
    }

    public void consumeRecords(DtsMessage dtsMessage) {
        long size = calculatorSize(dtsMessage);
        addUsedBytes(-size * SWELL_FACTOR);
    }

    public void consumeRecords(List<DtsMessage> records) {
        long size = calculatorSize(records);
        addUsedBytes(-size * SWELL_FACTOR);
    }

    private long getMaxWriterMemory() {
        maxWriterMemory = Math.min(MAX_WRITER_MEMORY.getValue(settings) * BYTES_PER_MB, maxAllowMemory);
        return maxWriterMemory;
    }

    private void waitUntilMemoryAvailable(long size) {
        try {
            while ((this.usedBytes.get() + size * SWELL_FACTOR) >= getMaxWriterMemory() && this.usedBytes.get() != 0) {
                LOG.info("Memory is not enough, will sleep, current: [{}], add size: [{}], factor:[{}], writer buffer:[{}]",
                        this.usedBytes.get(), size, SWELL_FACTOR, getMaxWriterMemory());
                setMemoryBlock(true);
                Thread.sleep(1000);
            }
            setMemoryBlock(false);
        } catch (InterruptedException e) {
            LOG.error("Interrupted when wai until memory available");
            throw new RuntimeException(e);
        }
    }

    private long addUsedBytes(long bytes) {
        return this.usedBytes.addAndGet(bytes);
    }

    private static long calculatorSize(DtsMessage dtsMessage) {
        if (dtsMessage == null) {
            return 0;
        }
        long size = 0;

        return dtsMessage.getSize();
    }

    private static long calculatorSize(List<DtsMessage> records) {
        if (CollectionUtils.isEmpty(records)) {
            return 0;
        }
        long size = 0;
        for (DtsMessage record : records) {
            size += record.getSize();
        }

        return size;
    }

    public boolean getMemoryBlock() {
        return this.memoryBlock;
    }

    public void setMemoryBlock(boolean memoryBlock) {
        this.memoryBlock = memoryBlock;
    }
}
