package com.aliyun.dts.deliver.connector.desination.internal;

import com.aliyun.dts.deliver.commons.util.Time;
import com.taobao.drc.togo.client.producer.SchemafulProducerRecord;

public class ProducerRecordWrapper {
    SchemafulProducerRecord schemafulProducerRecord;
    String topicName;
    String sourcePosition;
    long timestampMs;

    private long beginProduceTime = -1;
    private long endProduceTime = -1;

    public ProducerRecordWrapper(SchemafulProducerRecord schemafulProducerRecord, String topicName, String sourcePosition, long timestampMs) {
        this.schemafulProducerRecord = schemafulProducerRecord;
        this.topicName = topicName;
        this.sourcePosition = sourcePosition;
        this.timestampMs = timestampMs;
    }

    public SchemafulProducerRecord getSchemafulProducerRecord() {
        return schemafulProducerRecord;
    }

    public void setSchemafulProducerRecord(SchemafulProducerRecord schemafulProducerRecord) {
        this.schemafulProducerRecord = schemafulProducerRecord;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getSourcePosition() {
        return sourcePosition;
    }

    public void setSourcePosition(String sourcePosition) {
        this.sourcePosition = sourcePosition;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public void beginProduce() {
        beginProduceTime = Time.now();
    }

    public void doneProduce() {
        endProduceTime = Time.now();
    }

    public long getProduceTimeCostMS() {
        if (beginProduceTime != -1 && endProduceTime != -1) {
            return endProduceTime - beginProduceTime;
        } else {
            return 0;
        }
    }
}
