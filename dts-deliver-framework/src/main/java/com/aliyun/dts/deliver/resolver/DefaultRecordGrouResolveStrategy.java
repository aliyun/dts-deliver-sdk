package com.aliyun.dts.deliver.resolver;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.framework.dispatcher.ToExecuteRecordBatchQueue;
import com.aliyun.dts.deliver.framework.dispatcher.checkpoint.CheckpointManager;
import com.aliyun.dts.deliver.framework.dispatcher.record.RecordGroup;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatchType;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.protocol.record.util.RecordTools;
import com.aliyun.dts.deliver.resolver.internal.RecordGroupSender;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultRecordGrouResolveStrategy implements RecordGroupResolveStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRecordGrouResolveStrategy.class);

    public static final Settings.Setting<Long> INITIAL_TIMESTAMP_SETTING =
            Settings.longSetting("initialTimestamp",
                    "initial timestamp for delay and checkpoint calculate, if there is not data arrive",
                    -1L);

    public static final Settings.Setting<Long> RECORD_GROUP_ASSEMBLY_MAX_WAIT_MILLISECONDS = Settings.longSetting(
            "record.group.assembly.max.wait.milliseconds", "the record group assembly max wait milliseconds", 100L);

    public static final Settings.Setting<Integer> SYNC_CONFLICT_CAPACITY_SETTING =
            Settings.integerSetting("syncConflictCapacity",
                    "max holding record count in resolver",
                    10);

    private int groupSizeLimit;
    protected  volatile long last = -1;

    private ConcurrentMap<Long, Deque<RecordGroup>> batches;

    protected long maxWaitTimeMS;

    private AtomicInteger appendsInProgress;

    protected Time time;

    private AtomicInteger flushesInProgress;

    protected ToExecuteRecordBatchQueue independentRecordGroupQueue;

    protected ReentrantLock lock = new ReentrantLock();
    protected Condition condition = lock.newCondition();

    protected CheckpointManager checkpointManager;

    protected volatile int currentTicket;
    protected int totalTicket;
    protected int waitTicket = 0;

    private RecordGroupSender sender;

    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private Set<Long> muted;

    private int drainIndex;

    protected DtsContext context;

    @Override
    public void initialize(Settings config, ToExecuteRecordBatchQueue independentRecordGroupQueue, Metrics metrics, Time time, DtsContext context) {
        this.independentRecordGroupQueue = independentRecordGroupQueue;

//        Set<String> uniqueSources = context.getUniqueSources();
//        Preconditions.checkState(!uniqueSources.isEmpty());
        checkpointManager = new CheckpointManager(context.getStartCheckpoints());

        this.groupSizeLimit = GlobalSettings.RECORD_GROUP_ASSEMBLY_SIZE_LIMIT.getValue(config);
        Preconditions.checkState(groupSizeLimit >= 1);

        this.maxWaitTimeMS = RECORD_GROUP_ASSEMBLY_MAX_WAIT_MILLISECONDS.getValue(config);
        Preconditions.checkState(maxWaitTimeMS > 0);

        currentTicket = SYNC_CONFLICT_CAPACITY_SETTING.getValue(config);
        totalTicket = currentTicket;

        this.batches = new ConcurrentHashMap<>();

        this.time = time;

        this.appendsInProgress = new AtomicInteger(0);

        this.flushesInProgress = new AtomicInteger(0);

        //this.inFlightQueue = inFlightQueue;

        this.muted = new ConcurrentHashSet<>();

        this.sender = new RecordGroupSender(this, independentRecordGroupQueue, time);

        this.context = context;

        metrics.addMetric(metrics.metricName("keyInBucket", "RecordGroupResolveStrategy", "group in bucket RecordGroupResolveStrategy"), new Gauge<Integer>() {
            @Override
            public Integer value(MetricConfig config, long now) {
                return batches.size();
            }
        });

        metrics.addMetric(metrics.metricName("groupInBucket", "RecordGroupResolveStrategy", "group in bucket RecordGroupResolveStrategy"), new Gauge<Integer>() {
            @Override
            public Integer value(MetricConfig config, long now) {
                int count = 0;
                Collection<Deque<RecordGroup>> values = batches.values();
                for (Deque<RecordGroup> deque : values) {
                    count += deque.size();
                }
                return count;
            }
        });

        metrics.addMetric(metrics.metricName("recordsInBucket", "RecordGroupResolveStrategy", "records in bucket RecordGroupResolveStrategy"), new Gauge<Integer>() {
            @Override
            public Integer value(MetricConfig config, long now) {
                int recordCount = 0;
                Collection<Deque<RecordGroup>> values = batches.values();
                for (Deque<RecordGroup> deque : values) {
                    for (RecordGroup group : deque) {
                        recordCount += group.getRecordCount();
                    }
                }
                return recordCount;
            }
        });
    }

    /**
     * Resolve traceableRecord, such as ddl dml .
     *
     * @param dtsMessage
     */
    @Override
    public boolean resolve(DtsMessage dtsMessage) {
        try {
            lock.lock();

            Record record = dtsMessage.getRecord();

            switch (record.getOperationType()) {
                case BEGIN:
                case COMMIT:
                case ROLLBACK:
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                case DDL:
                case HEARTBEAT:
                    RecordAppendResult result = handleRecord(dtsMessage);

                    if (result.batchIsFull || result.newBatchCreated) {
                        this.sender.wakeup();
                    }
                    break;
                default:
                    throw new CriticalDtsException("nosql-replicate", ErrorCode.REPLICATE_UNSUPPORTED_OPERATION_TYPE,
                            "DefaultRecordGroupResolveStrategy: not supported record type :" + record.getOperationType());
            }

            return true;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            throw new CriticalDtsException("nosql-replicate", ErrorCode.REPLICATE_RESOLVE_OPERATION_TYPE_FAILED, "resolve failed");
        } finally {
            lock.unlock();
        }
    }

    protected RecordAppendResult handleRecord(DtsMessage dtsMessage) throws InterruptedException {
        // check whether the incoming transaction need to wait:
        // 1. if currentTicket >= record.size, don't wait
        // 2. if currentTicket == WriterConfig.conflict_capacity, which means the tree is empty, don't wait
        // 3. if currentTicket < record.size, and currentTicket < WriterConfig.conflict_capacity, that is,
        // there is not enough tickets for the transaction, and the tree is not empty, wait
        while (!context.isStopped() && currentTicket < 1 && currentTicket < totalTicket ) {
            // make sure waitTicket is no larger than WriterConfig.conflict_capacity, or the thread may never be waken up
            waitTicket = Math.min(1, totalTicket);
            condition.await(1, TimeUnit.SECONDS);
        }

        currentTicket -= 1;
        waitTicket = 0;

        //long key = dtsMessage.getGroupKey();
        Record record = dtsMessage.getRecord();
        String uniqueSourceName = record.getUniqueSourceName();
        long key = record.getGroupKey();

        try {
            Deque<RecordGroup> dq = getOrCreateDeque(uniqueSourceName, key);

            synchronized (dq) {
                RecordAppendResult appendResult = tryAppend(record.getTimestamp(), key, record, dq);

                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                RecordBatchType recordBatchTye =  RecordBatchType.dml;

                if (record.getOperationType().equals(OperationType.HEARTBEAT) ) {
                    recordBatchTye = RecordBatchType.heartbeat;
                }

                RecordGroup recordGroup = new RecordGroup(uniqueSourceName, recordBatchTye, key, this.groupSizeLimit, time.milliseconds());

                checkpointManager.start(recordGroup);

                recordGroup.tryAppend(record);

                //build dque
                RecordGroup last = dq.peekLast();

                dq.addLast(recordGroup);

                return new RecordAppendResult(true, dq.size() > 1 || recordGroup.isFull(), true);
            }
        } finally {
//            if (buffer != null) {
//                free.deallocate(buffer);
//            }
            appendsInProgress.decrementAndGet();
        }
    }

    private Deque<RecordGroup> getOrCreateDeque(String uniqueSourceName, long key) {
        //int uniqueSourceNameHashCode = uniqueSourceName.hashCode();
        long batchKey = key;
        Deque<RecordGroup> d = this.batches.get(batchKey);
        if (d != null) {
            return d;
        }
        d = new ArrayDeque<>();
        Deque<RecordGroup> previous = this.batches.putIfAbsent(batchKey, d);
        if (previous == null) {
            return d;
        } else {
            return previous;
        }
    }

    /**
     *  Try to append to a ProducerBatch.
     *
     *  If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     *  resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     *  and memory records built) in one of the following cases (whichever comes first): right before send,
     *  if it is expired, or when the producer is closed.
     */
    private RecordAppendResult tryAppend(long timestamp, long key, Record record, Deque<RecordGroup> deque) {
        RecordGroup last = deque.peekLast();
        if (last != null) {
            boolean result = last.tryAppend(record);
            if (result) {
                return new RecordAppendResult(result, deque.size() > 1 || last.isFull(), false);
            }
        }
        return null;
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /**
     * @param recordGroups
     * space and ref release in this method
     */
    @Override
    public void release(List<RecordGroup> recordGroups) {
        try {
            lock.lock();
            for (RecordGroup recordGroup : recordGroups) {
                switch (recordGroup.getRecordBatchType()) {
                    case dml:
                    case ddl:
                        releaseSingleRecordGroup(recordGroup);
                        break;
                }
            }
        } catch (Exception e) {
            LOG.error("release transaction error", e);
            throw new CriticalDtsException("nosql-replicate", ErrorCode.REPLICATE_RELEASE_TRANSACTION_FAILED,
                    "release transaction failed", e);
        } finally {
            lock.unlock();
        }
    }

    protected void releaseSingleRecordGroup(RecordGroup recordGroup) throws InterruptedException {
        checkpointManager.finish(recordGroup);

        //unmute the specified assembly key
        unmutePartition(recordGroup.getAssemblyKey());

        currentTicket += recordGroup.getRecordCount();

        if (currentTicket >= waitTicket) {
            condition.signal();
        }
    }

    @Override
    public List<Pair<String, RecordCheckpoint>> getSafeCheckpoint() {
        return checkpointManager.getMinimalCheckpoints();
    }

    @Override
    public void start() {
        this.sender.start();
    }

    @Override
    public void stop() {
        this.sender.stopSend();
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (Map.Entry<Long, Deque<RecordGroup>> entry : this.batches.entrySet()) {
            Deque<RecordGroup> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    /*
     * Metadata about a record just appended to the record batch
     */
    public final class RecordAppendResult {
        public final boolean result;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(boolean result, boolean batchIsFull, boolean newBatchCreated) {
            this.result = result;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final class ReadyCheckResult {
        public final Set<Long> readyKeys;
        public final long nextReadyCheckDelayMs;

        ReadyCheckResult(Set<Long> readyKeys, long nextReadyCheckDelayMs) {
            this.readyKeys = readyKeys;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
        }
    }

    public synchronized void mutePartition(Long key) {
        muted.add(key);
    }

    public synchronized void unmutePartition(long key) {
        muted.remove(key);
    }

    /**
     * Get a list of record assembly keys whose keys are ready to be sent, and the earliest time at which  will be ready;
     * <p>
     * A destination record assembly key is ready to send data if:
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(long nowMs) {
        Set<Long> readyKeys = new HashSet<>();
        long nextReadyCheckDelayMs = maxWaitTimeMS;

        //boolean exhausted = this.free.queued() > 0;
        boolean exhausted = (currentTicket == 0);

        for (Map.Entry<Long, Deque<RecordGroup>> entry : this.batches.entrySet()) {
            Long key = entry.getKey();
            Deque<RecordGroup> deque = entry.getValue();
            synchronized (deque) {
                if (!muted.contains(key)) {
                    RecordGroup batch = deque.peekFirst();
                    if (batch != null) {
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
                        long timeToWaitMs = this.maxWaitTimeMS;
                        boolean full = deque.size() > 1 || batch.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        boolean sendable = full || expired || exhausted || flushInProgress();
                        if (sendable) {
                            readyKeys.add(key);
                        } else {
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyKeys, nextReadyCheckDelayMs);
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified size
     **/
    public Map<Long, RecordGroup> drain(Set<Long> readyKeys, long now) {
        Map<Long, RecordGroup> batches = new HashMap<>();

        for (Long key : readyKeys) {
            if (!muted.contains(key)) {
                Deque<RecordGroup> deque = this.batches.get(key);
                if (deque != null) {
                    synchronized (deque) {
                        RecordGroup first = deque.peekFirst();
                        if (first != null) {
                            RecordGroup batch = deque.pollFirst();
                            batches.put(key, batch);
                        }
                    }
                }
            }
        }

        //mute drained key
        batches.keySet().forEach(key -> mutePartition(key));

        return batches;
    }

    //for test
    protected ConcurrentMap<Long, Deque<RecordGroup>> getBatches() {
        return this.batches;
    }
}

