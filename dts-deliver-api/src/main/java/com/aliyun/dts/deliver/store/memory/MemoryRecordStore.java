package com.aliyun.dts.deliver.store.memory;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class MemoryRecordStore extends AbstractRecordStoreWithMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryRecordStore.class);

    public static final Settings.Setting<Integer> SLOT_MASK_BIT = Settings.integerSetting(
        "memory.record.store.slot.mask.bit", "the slot mask bit to sharding record range", 2);
    public static final Settings.Setting<Long> SLOT_POLL_MAX_WAIT_MILLISECONDS = Settings.longSetting(
        "memory.record.store.slot.poll.max.wait.milliseconds", "the max polling wait milliseconds", 1000L);
    public static final Settings.Setting<Long> SLOT_POLL_MAX_WAIT_SLEEP_MILLISECONDS = Settings.longSetting(
            "memory.record.store.slot.poll.max.wait.sleep.milliseconds", "the milliseconds once time sleep", 100L);
    public static final Settings.Setting<Integer> BLOCKING_QUEUE_SIZE = Settings.integerSetting(
        "memory.record.store.blocking.queue.size", "the size for each blocking queue in memory store", 1024);

    private volatile boolean eof = false;

    private Slot[] slots;
    private int slotIndexMask;
    private long maxWaitMilliSeconds;
    private int blockingQueueSize;
    private long maxWaitMilliSecondsOnceSleep;

    private String checkpointString;
    private Long checkpointTimestamp;

    public MemoryRecordStore(Metrics metrics, DtsContext context) {
        super(metrics, context);
    }

    @Override
    public void initialize(Settings settings) {
        int slotBit = SLOT_MASK_BIT.getValue(settings);
        int slotSize = 1 << slotBit;

        slotIndexMask = slotSize - 1;

        slots = new Slot[slotSize];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot(i);
        }

        metrics.addMetric(
            metrics.metricName("recordCount", "memoryRecordStore"),
            (config, now) -> (pendingSize()));
        maxWaitMilliSeconds = SLOT_POLL_MAX_WAIT_MILLISECONDS.getValue(settings);

        blockingQueueSize = BLOCKING_QUEUE_SIZE.getValue(settings);
        maxWaitMilliSecondsOnceSleep = SLOT_POLL_MAX_WAIT_SLEEP_MILLISECONDS.getValue(settings);
    }

    @Override
    protected List<DtsMessage> realConsume(int supposedSize, int groupKey) {
        Slot slot = getSlot(groupKey);
        List<DtsMessage> records = slot.poll(maxWaitMilliSeconds, supposedSize, groupKey);
        this.memoryThrottle.consumeRecords(records);
        return records;
    }


    @Override
    protected void realPublish(DtsMessage dtsMessage) {
        long recordSize = memoryThrottle.beforePublishRecords(dtsMessage);

        Slot slot = getSlot(dtsMessage.getGroupKey());
        slot.append(dtsMessage);
        memoryThrottle.afterPublishRecords(recordSize);
    }

    private Slot getSlot(long groupKey) {
        int i = (int) groupKey & slotIndexMask;
        return slots[i];
    }


    class Slot {
        private ConcurrentHashMap<Long, BlockingQueue<DtsMessage>> recordsByGroupId = new ConcurrentHashMap<>();


        protected Slot(int index) {
            if (metrics != null) {
                metrics.addMetric(
                        metrics.metricName("slotIndex" + index + "RecordsByRangeIdCount", "memoryRecordStore"),
                        (config, now) -> (recordsByGroupId.size()));
                metrics.addMetric(
                        metrics.metricName("slotIndex" + index + "RecordCount", "memoryRecordStore"),
                        (config, now) -> (getRecordSize()));
            }
        }

        long getRecordSize() {
            return recordsByGroupId.values().stream().mapToLong(q -> q.size()).sum();
        }


        public boolean append(DtsMessage dtsMessage) {
            AtomicReference<Boolean> rs = new AtomicReference<>(true);
            BlockingQueue<DtsMessage> holdingRecords = recordsByGroupId.computeIfAbsent(dtsMessage.getGroupKey(), key -> {
                rs.set(false);
                return new LinkedBlockingQueue<>(blockingQueueSize);
            });

            SwallowException.callAndThrowRuntimeException(() -> holdingRecords.put(dtsMessage));
            return rs.get();
        }

        List<DtsMessage> poll(long waitMilliSeconds, int supposedSize, int groupKey) {
            List<DtsMessage> rs = null;
            int currentSize = 0;

            while (waitMilliSeconds > 0) {
                BlockingQueue<DtsMessage> holdingRecords = recordsByGroupId.get(groupKey);
                if (null == holdingRecords) {
                    // the record range is released by source
                    break;
                }
                if (holdingRecords.isEmpty()) {
                    SwallowException.callAndSwallowException(() -> Thread.sleep(maxWaitMilliSecondsOnceSleep));
                    waitMilliSeconds -= maxWaitMilliSecondsOnceSleep;
                    continue;
                }

                // allocate rs if it has not yet been initialize
                if (null == rs) {
                    rs = new ArrayList<>(supposedSize);
                }

                currentSize = holdingRecords.drainTo(rs, supposedSize);
                if (0 == currentSize) {
                    continue;
                }

                supposedSize -= currentSize;
                if (supposedSize < 1) {
                    break;
                }else if (memoryThrottle.getMemoryBlock()) {
                    break;
                }
            }

            return rs;
        }
    }

    protected long pendingSize() {
        return Arrays.stream(slots).mapToLong(slot -> slot.getRecordSize()).sum();
    }

    public Pair<String, Long> checkpoint() {
        return Pair.of(checkpointString, checkpointTimestamp);
    }

    @Override
    public boolean isEOF() {
        return false;
    }

    public void setCheckpoint(String checkpointString, Long checkpointTimestamp) {
        this.checkpointString = checkpointString;
        this.checkpointTimestamp = checkpointTimestamp;
    }

}
