package com.aliyun.dts.deliver.framework.processor;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.RowImage;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;

public class InflightRecord implements Record {
    final Record record;
    boolean isConsumed;

    public InflightRecord prev;
    public InflightRecord next;

    public InflightRecord(Record record) {
        this.record = record;
        this.isConsumed = false;
    }

    public Record getRecord() {
        return record;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    public void setConsumed(boolean consumed) {
        isConsumed = consumed;
    }

    public InflightRecord getPrev() {
        return prev;
    }

    public void setPrev(InflightRecord prev) {
        this.prev = prev;
    }

    public InflightRecord getNext() {
        return next;
    }

    public void setNext(InflightRecord next) {
        this.next = next;
    }

    public long getId() {
        return record.getId();
    }

    @Override
    public String getTransactionId() {
        return record.getTransactionId();
    }

    @Override
    public long getTimestamp() {
        return record.getTimestamp();
    }

    @Override
    public long getBornTimestamp() {
        return record.getBornTimestamp();
    }

    @Override
    public String getCheckpoint() {
        return record.getCheckpoint();
    }

    @Override
    public OperationType getOperationType() {
        return record.getOperationType();
    }

    public RecordSchema getSchema() {
        return record.getSchema();
    }

    @Override
    public RowImage getBeforeImage() {
        return record.getBeforeImage();
    }

    @Override
    public RowImage getAfterImage() {
        return record.getAfterImage();
    }

    @Override
    public Set<String> getRawFieldNames() {
        return record.getRawFieldNames();
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        return record.getExtendedProperty();
    }

    public long size() {
        return record.size();
    }

    @Override
    public boolean isKeyChanged() {
        return record.isKeyChanged();
    }

    @Override
    public Pair<String, Object> getRecordRawData() {
        return record.getRecordRawData();
    }

    @Override
    public Pair<String, String> getSourceTypeAndVersion() {
        return record.getSourceTypeAndVersion();
    }

    @Override
    public String getUniqueSourceName() {
        return record.getUniqueSourceName();
    }

    public String toString() {
        return "InflightRecord[raw Record: " + record + "]";
    }

    public long getGroupKey() {
        return record.getGroupKey();
    }

    @Override
    public RecordCheckpoint getRecordCheckpoint() {
        return record.getRecordCheckpoint();
    }
}
