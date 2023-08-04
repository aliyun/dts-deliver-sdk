package com.aliyun.dts.deliver.framework.dispatcher.record;

import com.aliyun.dts.deliver.framework.dispatcher.checkpoint.CheckpointTrait;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatch;
import com.aliyun.dts.deliver.framework.dispatcher.record.batch.RecordBatchType;
import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractRecordBatch<T> extends CheckpointTrait implements RecordBatch {

    protected RecordBatchType recordBatchType;

    protected List<T> itemList;
    protected int maxRecords = 1024;

    protected Map<String, String> extendedProperty;

    /**
     * The first record in RecordBatch, this item can help us to implement checkpoint related methods
     */
    private Record firstRecord;

    private long totalRecordRawBytes;

    public AbstractRecordBatch(RecordBatchType type) {
        this.recordBatchType = type;

        itemList = new LinkedList<>();
    }

    public void addRecord(Record record) {
        totalRecordRawBytes += record.size();

        if (null == firstRecord) {
            firstRecord = record;
        }
    }

    public void clearRecords() {
        itemList.clear();
        firstRecord = null;
    }

    public int getRecordCount() {
        return itemList.size();
    }

    @Override
    public RecordBatchType getRecordBatchType() {
        return recordBatchType;
    }

    public boolean isFull() {
        return itemList.size() >= maxRecords;
    }

    public long getTotalRecordRawBytes() {
        return totalRecordRawBytes;
    }

    @Override
    public RecordCheckpoint computeSafeCheckpoint() {
        if (null != firstRecord) {
            return firstRecord.getRecordCheckpoint();
        }

        return null;
    }


    @Override
    public Record getHeartbeatRecord() {
        Record rs = firstRecord;

        if (null != rs) {
            if (OperationType.HEARTBEAT != rs.getOperationType()) {
                rs = null;
            }
        }

        return rs;
    }
}
