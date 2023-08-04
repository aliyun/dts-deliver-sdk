package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.RowImage;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultRecord implements Record {

    protected final Optional<RecordHeader> recordHeader;
    private long recordHeaderSize;
    private final OperationType operationType;
    private final RecordSchema recordSchema;

    private RowImage beforeImage;
    private long beforeRowImageSize;
    private RowImage afterImage;
    private long afterRowImageSize;
    private Map<String, String> extendedProperty;
    private static Record eofRecord;

    private Pair<String, String> sourceTypeAndVersion;

    private long groupKey;

    private RecordCheckpoint recordCheckpoint;

    public static Record EOFRecord() {
        if (null == eofRecord) {
            eofRecord = new DefaultRecord(OperationType.EOF, null);
        }

        return eofRecord;
    }

    public DefaultRecord(OperationType operationType, RecordSchema recordSchema, RecordHeader recordHeader) {
        this.operationType = operationType;
        this.recordSchema = recordSchema;
        if (null == recordHeader) {
            this.recordHeader = Optional.empty();
            this.recordHeaderSize = 0L;
        } else {
            this.recordHeader = Optional.of(recordHeader);
            this.recordHeaderSize = recordHeader.size();
        }

        if (recordHeader != null) {
            this.sourceTypeAndVersion = Pair.of(recordHeader.getSource(), String.valueOf(recordHeader.getVersion()));
        } else {
            this.sourceTypeAndVersion = Pair.of("mysql", "5.7.1");
        }
    }

    public DefaultRecord(OperationType operationType, RecordSchema recordSchema) {
        this(operationType, recordSchema, null);
    }

    @Override
    public long getId() {
        return recordHeader.map(RecordHeader::getId)
            .orElse(0L);
    }

    @Override
    public String getTransactionId() {
        return recordHeader.map(RecordHeader::getSourceTxid)
            .orElse(null);
    }

    @Override
    public long getTimestamp() {
        return recordHeader.map(RecordHeader::getSourceTimestamp)
            .orElse(0L);
    }

    @Override
    public String getSourcePosition() {
        return recordHeader.map(RecordHeader::getSourcePosition).orElse(null);
    }

    @Override
    public String getSafeSourcePosition() {
        return recordHeader.map(RecordHeader::getSafeSourcePosition).orElse(null);
    }

    @Override
    public String getUniqueSourceName() {
        return recordHeader.map(RecordHeader::getUniqueSourceName).orElse(null);
    }

    public void setGroupKey(long groupKey) {
        this.groupKey = groupKey;
    }

    @Override
    public long getGroupKey() {
        return groupKey;
    }

    @Override
    public long getBornTimestamp() {
        return getTimestamp();
    }

    /**
     * Get the checkpoint of current record. the format of checkpoint is like following:
     * <p>
     * instance@record-id@position@timestamp-of-millisecond, such as 0@258907231@5830@0@1570864790000,
     * 0 is the instance,
     * 258907231 is the record id,
     * 5830@0 is the record position in source, please notice the position contains '@' by coincidence,
     * 1570864790000 is the timestamp in unit of millisecond.
     */
    @Override
    public String getCheckpoint() {
        String checkpoint = "";

        checkpoint = recordHeader
            .map(recordHeader ->
                Record.composeCheckpoint(
                    "0",
                    recordHeader.getId(),
                    recordHeader.getSourcePosition(),
                    getTimestamp()))
            .orElse("");

        return checkpoint;
    }

    @Override
    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }

    @Override
    public RowImage getBeforeImage() {
        return beforeImage;
    }

    public void setBeforeImage(RowImage rowImage) {
        this.beforeImage = rowImage;
        this.beforeRowImageSize = rowImage.size();
    }

    @Override
    public RowImage getAfterImage() {
        return afterImage;
    }

    @Override
    public Set<String> getRawFieldNames() {
        return recordSchema.getFieldNames().stream()
            .collect(Collectors.toSet());
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        return extendedProperty;
    }

    @Override
    public Pair<String, String> getSourceTypeAndVersion() {
        return sourceTypeAndVersion;
    }

    public void setExtendedProperty(String key, String value) {
        setExtendedProperties(Collections.singletonMap(key, value));
    }

    public void setExtendedProperties(Map<String, String> properties) {
        if (null == properties) {
            return;
        }
        if (null == extendedProperty) {
            extendedProperty = new TreeMap<>();
        }

        extendedProperty.putAll(properties);
    }

    public void setAfterImage(RowImage rowImage) {
        this.afterImage = rowImage;
        this.afterRowImageSize = rowImage.size();
    }

    public void setSourceTypeAndVersion(Pair<String, String> value) {
        this.sourceTypeAndVersion = value;
    }

    @Override
    public long size() {
        return this.recordHeaderSize + this.beforeRowImageSize + this.afterRowImageSize;
    }

    public Optional<RecordHeader> getRecordHeader() {
        return recordHeader;
    }

    @Override
    public RecordCheckpoint getRecordCheckpoint() {
        return recordCheckpoint;
    }

    public void setRecordCheckpoint(RecordCheckpoint recordCheckpoint) {
        this.recordCheckpoint = recordCheckpoint;
    }

    @Override
    public String toString() {
        StringBuilder sbl = new StringBuilder();

        sbl.append("DefaultRecord {");

        if (recordHeader.isPresent()) {
            sbl.append(recordHeader);
        }

        sbl.append(getSchema());
        sbl.append("beforeImage: ").append(getBeforeImage());
        sbl.append("afterImage: ").append(getAfterImage());

        sbl.append("}");

        return sbl.toString();
    }
}
