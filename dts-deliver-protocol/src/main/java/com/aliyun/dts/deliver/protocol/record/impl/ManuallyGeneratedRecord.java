package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.RowImage;
import com.aliyun.dts.deliver.protocol.record.value.StringValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ManuallyGeneratedRecord implements Record {

    public static final String MANUALLY_GENERATES_RECORD_FILE_NAME = "manuallyField";
    public static final RecordSchema MANUALLY_GENERATES_RECORD_SCHEMA =  new DefaultRecordSchema(null, null, null,
            Arrays.asList(
                    DefaultRecordField.builder().withFieldName(MANUALLY_GENERATES_RECORD_FILE_NAME).withRawDataType(DefaultRawDataType.of("String", -1, false)).get()
            )
        );

    private final String payLoad;

    public ManuallyGeneratedRecord(String payload) {
        this.payLoad = payload;
    }

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public String getTransactionId() {
        return null;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }

    @Override
    public long getBornTimestamp() {
        return 0;
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.MANUAL_GENERATED;
    }

    @Override
    public RecordSchema getSchema() {
        return MANUALLY_GENERATES_RECORD_SCHEMA;
    }

    @Override
    public RowImage getBeforeImage() {
        return null;
    }

    @Override
    public RowImage getAfterImage() {
        DefaultRowImage ret =  new DefaultRowImage(MANUALLY_GENERATES_RECORD_SCHEMA);
        ret.setValue(MANUALLY_GENERATES_RECORD_FILE_NAME, new StringValue(payLoad));
        return ret;
    }

    @Override
    public Set<String> getRawFieldNames() {
        return Collections.singleton(MANUALLY_GENERATES_RECORD_FILE_NAME);
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        return null;
    }

    @Override
    public long size() {
        return null == payLoad ? 0 : payLoad.length();
    }
}
