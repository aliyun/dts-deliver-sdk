package com.aliyun.dts.deliver.resolver.internal;

import com.aliyun.dts.deliver.framework.dispatcher.ToExecuteRecordBatchQueue;
import com.aliyun.dts.deliver.framework.dispatcher.record.RecordGroup;
import com.aliyun.dts.deliver.resolver.DefaultRecordGrouResolveStrategy;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RecordGroupSender extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(RecordGroupSender.class);

    private DefaultRecordGrouResolveStrategy recordGrouResolveStrategy;

    private ToExecuteRecordBatchQueue independentTransactions;

    private Time time;

    protected ReentrantLock lock = new ReentrantLock();
    protected Condition condition = lock.newCondition();

    private volatile boolean isStopped;

    public RecordGroupSender(DefaultRecordGrouResolveStrategy recordGrouResolveStrategy, ToExecuteRecordBatchQueue independentTransactions, Time time) {
        this.setName("Record group sender");
        this.setDaemon(true);

        this.recordGrouResolveStrategy = recordGrouResolveStrategy;
        this.independentTransactions = independentTransactions;
        this.time = time;
    }

    @Override
    public void run() {
        while (!isStopped()) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                LOG.error("Uncaught error in RecordGroupDispatcher thread: ", e);
            }
        }
    }

    /**
     * Run a single iteration of sending
     *
     * @param now The current POSIX time in milliseconds
     */
    void run(long now) throws InterruptedException {
        lock.lock();
        try {
            //get ready assembly record keys
            DefaultRecordGrouResolveStrategy.ReadyCheckResult checkResult = recordGrouResolveStrategy.ready(now);

            if (checkResult.readyKeys.isEmpty()) {
                //Thread.sleep(checkResult.nextReadyCheckDelayMs);
                condition.await(1, TimeUnit.MILLISECONDS);
                return;
            }

            // create produce requests
            Map<Long, RecordGroup> recordGroups = recordGrouResolveStrategy.drain(checkResult.readyKeys, now);

            int sucessInNum = 0;
            long partitionKey;
            RecordGroup recordGroup;
            for (Map.Entry<Long, RecordGroup> entry : recordGroups.entrySet()) {
                partitionKey = entry.getKey();
                recordGroup = entry.getValue();
                Preconditions.checkState(independentTransactions.in(recordGroup));
            }
        } finally {
            lock.unlock();
        }
    }

    public void wakeup() {

        if (lock.tryLock()) {
            try {
                this.condition.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    public synchronized void stopSend() {
        isStopped = true;
    }

    public synchronized boolean isStopped() {
        return isStopped;
    }

}
