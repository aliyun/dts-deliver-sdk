package com.aliyun.dts.deliver.framework.dispatcher.record;

import com.aliyun.dts.deliver.commons.util.Time;
import com.aliyun.dts.deliver.framework.dispatcher.checkpoint.CheckpointTrait;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatch;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatchType;
import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordGroup extends CheckpointTrait implements RecordBatch {

    private long assemblyKey;

    private long startTime;

    private long lastAttemptMs;

    private AtomicInteger consumedRecordNum;

    protected RecordBatchType recordBatchType;

    protected List<Record> itemList;
    protected int maxRecords = 1024;

    protected Map<String, String> extendedProperty;

    /**
     * The last record in RecordBatch, this item can help us to implement checkpoint related methods
     */

    private Record lastRecord;

    private long totalRecordRawBytes;

    private String uniqueSourceName;

    public RecordGroup(RecordBatchType type) {
        this(type, 0);
    }

    public RecordGroup(RecordBatchType type, int assemblyKey) {
        this.recordBatchType = type;
        this.assemblyKey = assemblyKey;
        itemList = new LinkedList<>();
    }

    public RecordGroup(String uniqueSourceName, RecordBatchType type, long assemblyKey, int maxRecords, long now) {
        this.uniqueSourceName = uniqueSourceName;

        this.recordBatchType = type;

        this.assemblyKey = assemblyKey;
        this.startTime = Time.now();
        this.maxRecords = maxRecords;
        this.consumedRecordNum = new AtomicInteger(0);
        this.lastAttemptMs = now;
        itemList = new LinkedList<>();
    }

    public void addRecord(Record record) {
        totalRecordRawBytes += record.size();

        this.lastRecord = record;

        this.itemList.add(record);
    }

    public boolean tryAppend(Record record) {
        if (isFull()) {
            return false;
        } else {
            this.addRecord(record);
            return true;
        }
    }

    @Override
    public RecordBatchType getRecordBatchType() {
        return recordBatchType;
    }

    public List<Record> getRecordList() {
        return this.itemList;
    }

    public long getAssemblyKey() {
        return assemblyKey;
    }

    public void setAssemblyKey(int assemblyKey) {
        this.assemblyKey = assemblyKey;
    }

    public void addConsumedNum() {
        this.consumedRecordNum.incrementAndGet();
    }

    public boolean allConsumed() {
        return consumedRecordNum.intValue() == itemList.size();
    }

    public long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    public boolean isFull() {
        return itemList.size() >= maxRecords;
    }

    @Override
    public RecordCheckpoint computeSafeCheckpoint() {
        if (null != lastRecord) {
            return lastRecord.getRecordCheckpoint();
        }

        return null;
    }

    @Override
    public Record getHeartbeatRecord() {
        Record rs = lastRecord;

        if (null != rs) {
            if (OperationType.HEARTBEAT != rs.getOperationType()) {
                rs = null;
            }
        }

        return rs;
    }


    public int getRecordCount() {
        return itemList.size();
    }

    public String getUniqueSourceName() {
        return uniqueSourceName;
    }
}
