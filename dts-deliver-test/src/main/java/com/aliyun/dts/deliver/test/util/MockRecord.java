package com.aliyun.dts.deliver.test.util;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.impl.DefaultRecord;
import com.aliyun.dts.deliver.protocol.record.impl.RecordHeader;
import com.aliyun.dts.deliver.protocol.record.util.RecordTools;
import org.apache.commons.lang3.tuple.Pair;

public class MockRecord extends DefaultRecord {

    Pair<String, String> sourceTypeAndVersion = Pair.of("mysql", "5.7.10");

    public MockRecord(OperationType operationType, RecordSchema recordSchema, RecordHeader recordHeader) {
        super(operationType, recordSchema, recordHeader);
    }

//    public MockRecord(OperationType operationType, RecordSchema recordSchema, long timestamp) {
//        super(operationType, recordSchema);
//        this.timestamp = timestamp;
//    }

    @Override
    public Pair<String, String> getSourceTypeAndVersion() {
        return sourceTypeAndVersion;
    }

    public void setSourceTypeAndVersion(Pair<String, String> value) {
        sourceTypeAndVersion = value;
    }

    @Override
    public boolean isKeyChanged() {
        return  RecordTools.isKeyChange(this);
    }

    public long size() {
        return 1024;
    }
}
