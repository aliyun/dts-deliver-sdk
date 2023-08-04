package com.aliyun.dts.deliver.connector.desination.internal;

import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.taobao.drc.togo.client.producer.SchemafulProducerRecord;
import com.taobao.drc.togo.common.businesslogic.RecordType;
import com.taobao.drc.togo.data.Data;
import com.taobao.drc.togo.data.SchemafulRecord;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;

public class DStoreRecordBuilder {

    private DirectRecordCachedSerializer directRecordSerializer;

    public DStoreRecordBuilder(boolean serializerUseCache) {
        this.directRecordSerializer = new DirectRecordCachedSerializer(serializerUseCache);
    }

    public SchemafulProducerRecord build(String topicName, int partition, Schema schema, SchemaMetaData schemaMetaData, Record record, boolean needTimestamp) throws IOException {

        byte[] data = directRecordSerializer.serialize(record);
        Map<String, String> tags = record.getExtendedProperty();

        String dbName = "";
        String tbName = "";
        RecordSchema recordSchema = record.getSchema();
        if (null != recordSchema) {
            dbName = recordSchema.getDatabaseName().orElse(null);
            tbName = recordSchema.getTableName().orElse(null);
        }

        SchemafulRecord schemafulRecord = Data.get().newRecord(schema)
                .put("dbName", dbName)
                .put("tbName", tbName)
                .put("threadID", getLongAttribute(tags, "thread_id"))
                .put("extraTag", 0L)
                //.put("extraTag", BitUtil.formatHashRegionIDAndRecordType(getBusinessHashCode(record),getRegionID(record),getRecordType(record)))
                .put("timestamp", record.getTimestamp())
                .put("extraIndex", getLongAttribute(tags, "extra_index"));
        if (needTimestamp) {
            return new SchemafulProducerRecord(topicName, partition, System.currentTimeMillis()/*getTimestampMs(record)*/, schemafulRecord, schemaMetaData, data);
        } else {
            return new SchemafulProducerRecord(topicName, partition, schemafulRecord, schemaMetaData, data);
        }
    }

    /**
     * 获取Record中的String属性信息
     * @param attributes
     * @return
     */
    private String getStringAttribute(Map<String, String> attributes, String attributeName) {
        return  (null == attributes || !attributes.containsKey(attributeName)) ? "" : attributes.get(attributeName);
    }

    private long getLongAttribute(Map<String, String> attributes, String attributeName) {
        return  (null == attributes || !attributes.containsKey(attributeName)) ? 0L : Long.parseLong(attributes.get(attributeName));
    }

    /**
     * 获取Record中的business_hash_code
     * @param record
     * @return
     */
    private int getBusinessHashCode(Record record) {
        String businessHashCode = getStringAttribute(record.getExtendedProperty(), "business_hash_code");
        if (StringUtils.isEmpty(businessHashCode)) {
            return record.hashCode();
        } else {
            return  Integer.parseInt(businessHashCode);
        }
    }

    /**
     * 获取Record中的region_id
     * @param record
     * @return
     */
    private short getRegionID(Record record) {
        String regionId = getStringAttribute(record.getExtendedProperty(), "region_id");
        if (StringUtils.isEmpty(regionId)) {
            return 0;
        } else {
            return Short.parseShort(regionId);
        }
    }

    /**
     * 根据本记录的操作类型（operation）转换为相应的RecordType（see enum RecordType）
     * (注：未对DB类型进行细分,默认参考mysql类型进行处理)
     * @return
     */
    private short getRecordType(Record record) {
        short retType = RecordType.UNKNOWN.getTypeID();
        switch (record.getOperationType()) {
            case INSERT:
            case UPDATE:
            case DELETE:
                retType = RecordType.DML.getTypeID();
                break;
            case HEARTBEAT:
                retType = RecordType.HEARTBEAT.getTypeID();
                break;
            case BEGIN:
                retType = RecordType.BEGIN.getTypeID();
                break;
            case COMMIT:
            case ABORT:
                retType = RecordType.COMMIT.getTypeID();
                break;
            case DDL:
                retType = RecordType.DDL.getTypeID();
                break;
            default:
                retType = RecordType.OTHER.getTypeID();
                break;
        }
        return retType;
    }
}
