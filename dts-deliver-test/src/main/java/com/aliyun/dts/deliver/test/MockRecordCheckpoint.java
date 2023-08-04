package com.aliyun.dts.deliver.test;

import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;

public class MockRecordCheckpoint implements RecordCheckpoint {

    private String value;

    public MockRecordCheckpoint(String value) {
        this.value = value;
    }

    @Override
    public String getSerializedValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
