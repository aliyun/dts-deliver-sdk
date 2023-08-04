package com.aliyun.dts.deliver.connector.desination.internal;

import com.aliyun.dts.deliver.commons.util.ObjectNameUtils;
import com.aliyun.dts.deliver.protocol.avro.generated.EmptyObject;
import com.aliyun.dts.deliver.protocol.avro.generated.Operation;
import com.aliyun.dts.deliver.protocol.avro.generated.Source;

import com.aliyun.dts.deliver.protocol.avro.generated.SourceType;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordField;
import com.aliyun.dts.deliver.protocol.record.RecordIndexInfo;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.RowImage;
import com.aliyun.dts.deliver.protocol.record.value.*;
import org.apache.avro.io.BinaryEncoder;


import org.apache.avro.io.CachedBufferedBinaryEncoder;
import org.apache.avro.io.CachedEncoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Integer;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class DirectRecordCachedSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(DirectRecordCachedSerializer.class);

    static final int[] AVRO_OPERATION_ORDINALS = new int[24];

    static {
        AVRO_OPERATION_ORDINALS[OperationType.INSERT.ordinal()] = Operation.INSERT.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.UPDATE.ordinal()] = Operation.UPDATE.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.DELETE.ordinal()] = Operation.DELETE.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.DDL.ordinal()] = Operation.DDL.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.BEGIN.ordinal()] = Operation.BEGIN.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.COMMIT.ordinal()] = Operation.COMMIT.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.ROLLBACK.ordinal()] = Operation.ROLLBACK.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.ABORT.ordinal()] = Operation.ABORT.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.HEARTBEAT.ordinal()] = Operation.HEARTBEAT.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.CHECKPOINT.ordinal()] = Operation.CHECKPOINT.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.COMMAND.ordinal()] = Operation.COMMAND.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.FILL.ordinal()] = Operation.FILL.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.FINISH.ordinal()] = Operation.FINISH.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.CONTROL.ordinal()] = Operation.CONTROL.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.RDB.ordinal()] = Operation.RDB.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.NOOP.ordinal()] = Operation.NOOP.ordinal();
        AVRO_OPERATION_ORDINALS[OperationType.INIT.ordinal()] = Operation.INIT.ordinal();
    }

    private Source source;
    private ByteArrayOutputStream outputStream = null;
    private CachedBufferedBinaryEncoder binaryEncoder;
    private boolean useCache;

    public DirectRecordCachedSerializer(boolean useCache) {
        this.outputStream = new ByteArrayOutputStream(4096);
        this.useCache = useCache;
    }

    public byte[] serialize(Record record) throws IOException {
        this.outputStream.reset();
        this.binaryEncoder = CachedEncoderFactory.getDefault().binaryEncoder(outputStream, this.binaryEncoder, this.useCache);

        binaryEncoder.writeInt(0); // version
        binaryEncoder.writeLong(record.getId()); // id
        binaryEncoder.writeLong(record.getTimestamp()); // sourceTimestamp
        binaryEncoder.writeAsciiString(record.getSourcePosition()); // sourcePosition
        if (null != record.getSafeSourcePosition()) {
            binaryEncoder.writeAsciiString(record.getSafeSourcePosition()); // safeSourcePosition
        } else {
            binaryEncoder.writeAsciiString(""); // safeSourcePosition default null
        }
        if (null != record.getTransactionId()) {
            binaryEncoder.writeAsciiString(record.getTransactionId()); // sourceTxid
        } else {
            binaryEncoder.writeAsciiString(""); // sourceTxid default ""
        }

        /**
         * com.aliyun.dts.deliver.protocol.avro.generated.Source
         */
        this.serializeSource(record.getSourceTypeAndVersion(), binaryEncoder);

        /**
         * com.aliyun.dts.deliver.protocol.avro.generated.Operation
         */
        binaryEncoder.writeEnum(AVRO_OPERATION_ORDINALS[record.getOperationType().ordinal()]);

        RecordSchema recordSchema = record.getSchema();
        this.serializeObjectName(recordSchema, binaryEncoder);
        this.serializeProcessTimestamps(binaryEncoder);
        this.serializeTags(record.getExtendedProperty(), this.buildPkUkInfo(recordSchema), binaryEncoder);

        if (OperationType.DDL != record.getOperationType()) {
            List<RecordField> recordFields = this.serializeFields(recordSchema, binaryEncoder);
            this.serializeRowImage(recordFields, record.getBeforeImage(), binaryEncoder);
            this.serializeRowImage(recordFields, record.getAfterImage(), binaryEncoder);
        } else {
            List<RecordField> recordFields = this.serializeFields(null, binaryEncoder);
            this.serializeRowImage(recordFields, record.getBeforeImage(), binaryEncoder);
            this.serializeDDLImage(recordFields, record.getAfterImage(), binaryEncoder);
        }
        binaryEncoder.writeLong(record.getBornTimestamp());
        binaryEncoder.flush();
        return outputStream.toByteArray();
    }

    protected void serializeSource(Pair<String, String> typeAndVersion, BinaryEncoder encoder)
            throws IOException {
        if (null == this.source) {
            for (SourceType sourceType : SourceType.values()) {
                if (sourceType.name().equalsIgnoreCase(typeAndVersion.getKey())) {
                    this.source = new Source(sourceType, typeAndVersion.getValue());
                    break;
                }
            }
            if (null == this.source) {
                throw new IllegalArgumentException("No enum constant com.aliyun.dts.deliver.protocol.avro.generated.SourceType" + typeAndVersion.getKey());
            }
        }
        encoder.writeEnum(this.source.getSourceType().ordinal());
        encoder.writeString(this.source.getVersion());
    }

    protected void serializeObjectName(RecordSchema recordSchema, CachedBufferedBinaryEncoder encoder) throws IOException {

        String objectName = null;
        if (null != recordSchema) {
            String schemaName = recordSchema.getSchemaName().orElse(null);
            String tableName = recordSchema.getTableName().orElse(null);
            if (null != schemaName) {
                if (null != tableName) {
                    objectName = ObjectNameUtils.compressionObjectName(schemaName, tableName);
                } else {
                    objectName = schemaName;
                }
            }
        }
        if (null == objectName) {
            encoder.writeIndex(0);
            encoder.writeNull();
        } else {
            encoder.writeIndex(1);
            encoder.writeString(objectName, true);
        }
    }

    /**
     * 当前该字段未使用
     *
     * @param encoder
     * @throws IOException
     */
    protected void serializeProcessTimestamps(BinaryEncoder encoder) throws IOException {
        encoder.writeIndex(0);
        encoder.writeNull();
    }

    protected String buildPkUkInfo(RecordSchema recordSchema) {
        if (null != recordSchema) {
            RecordIndexInfo priIndexInfo = recordSchema.getPrimaryIndexInfo();
            List<RecordIndexInfo> uniqueIndexInfos = recordSchema.getUniqueIndexInfo();
            if (null != priIndexInfo || (null != uniqueIndexInfos && (!uniqueIndexInfos.isEmpty()))) {
                StringBuilder pkUkInfoBuilder = new StringBuilder(128);
                pkUkInfoBuilder.append("{");
                if (null != priIndexInfo) {
                    pkUkInfoBuilder.append("\"PRIMARY\":[");
                    priIndexInfo.getIndexFields().forEach(recordField -> pkUkInfoBuilder.append("\"").append(recordField.getFieldName()).append("\","));
                    pkUkInfoBuilder.setLength(pkUkInfoBuilder.length() - 1);
                    pkUkInfoBuilder.append("],");
                }

                if (null != uniqueIndexInfos) {
                    uniqueIndexInfos.forEach(indexInfo -> {
                        pkUkInfoBuilder.append("\"").append(indexInfo.getName()).append("\":[");
                        indexInfo.getIndexFields().forEach(recordField -> pkUkInfoBuilder.append("\"").append(recordField.getFieldName()).append("\","));
                        pkUkInfoBuilder.setLength(pkUkInfoBuilder.length() - 1);
                        pkUkInfoBuilder.append("],");
                    });
                }
                pkUkInfoBuilder.setLength(pkUkInfoBuilder.length() - 1);
                pkUkInfoBuilder.append("}");

                return pkUkInfoBuilder.toString();
            }
        }
        return null;
    }

    protected void serializeTags(Map<String, String> extendedProperty, String pkUkInfo, CachedBufferedBinaryEncoder encoder) throws IOException {
        encoder.writeMapStart();
        boolean empPkUk = StringUtils.isEmpty(pkUkInfo);
        int tagCnt = (null == extendedProperty ? 0 : extendedProperty.size()) + (empPkUk ? 0 : 1);
        if (tagCnt <= 0) {
            encoder.setItemCount(0);
        } else {
            encoder.setItemCount(tagCnt);
            if (!empPkUk) {
                encoder.startItem();
                encoder.writeString("pk_uk_info", true);
                encoder.writeString(pkUkInfo, true);
            }
            if (null != extendedProperty) {
                for (Map.Entry<String, String> keyValue : extendedProperty.entrySet()) {
                    encoder.startItem();
                    encoder.writeString(keyValue.getKey());
                    encoder.writeString(keyValue.getValue());
                }
            }
        }
        encoder.writeMapEnd();
    }

    protected List<RecordField> serializeFields(RecordSchema recordSchema, CachedBufferedBinaryEncoder encoder) throws IOException {
        List<RecordField> recordFields = null == recordSchema ? null : recordSchema.getFields();
        if (null == recordFields) {
            encoder.writeIndex(0);
            encoder.writeNull();
            return null;
        } else {
            encoder.writeIndex(2);
            encoder.writeArrayStart();
            encoder.setItemCount(recordFields.size());

            for (RecordField recordField : recordFields) {
                encoder.startItem();
                encoder.writeString(recordField.getFieldName(), true);
                encoder.writeInt(recordField.getOriginalColumnTypeNumber());
            }
            encoder.writeArrayEnd();
            return recordFields;
        }
    }

    protected static void serializeDateTime(BinaryEncoder encoder, DateTime dateTime)
            throws IOException {

        if (dateTime.isSet(DateTime.SEG_YEAR)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getYear());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_MONTH)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getMonth());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_DAY)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getDay());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_HOUR)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getHour());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_MINITE)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getMinute());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_SECOND)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getSecond());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }

        if (dateTime.isSet(DateTime.SEG_NAONS)) {
            encoder.writeIndex(1);
            encoder.writeInt(dateTime.getNaons());
        } else {
            encoder.writeIndex(0);
            encoder.writeNull();
        }
    }

    interface ValueSerializer {
        void serialize(CachedBufferedBinaryEncoder encoder, Value value) throws IOException;
    }

    static ValueSerializer[] valueSerializers = new ValueSerializer[ValueType.values().length];

    static {

        /**
         * code: 1
         * name: com.aliyun.dts.deliver.protocol.avro.generated.Integer
         */
        valueSerializers[ValueType.INTEGER_NUMERIC.ordinal()] = (encoder, value) -> {
            IntegerNumeric integerNumeric = (IntegerNumeric) value;

            encoder.writeIndex(1);
            encoder.writeInt(0);
            encoder.writeAsciiString(integerNumeric.toString());
        };

        /**
         * code: 2
         * name: com.aliyun.dts.deliver.protocol.avro.generated.Character
         */
        valueSerializers[ValueType.STRING.ordinal()] = (encoder, value) -> {
            StringValue stringValue = (StringValue) value;

            encoder.writeIndex(2);
            encoder.writeString(stringValue.getCharset(), true);
            encoder.writeBytes(stringValue.getData());
        };

        /**
         * code: 3
         * name: com.aliyun.dts.deliver.protocol.avro.generated.Decimal
         */
        valueSerializers[ValueType.DECIMAL_NUMERIC.ordinal()] = (encoder, value) -> {

            DecimalNumeric decimalNumeric = (DecimalNumeric) value;
            BigDecimal decimal = decimalNumeric.getData();

            encoder.writeIndex(3);
            encoder.writeAsciiString(decimal.toString());
            encoder.writeInt(decimal.precision());
            encoder.writeInt(decimal.scale());
        };

        valueSerializers[ValueType.SPECIAL_NUMERIC.ordinal()] = (encoder, value) -> {

            SpecialNumeric decimalNumeric = (SpecialNumeric) value;
            encoder.writeIndex(3);
            encoder.writeAsciiString(decimalNumeric.toString());
            encoder.writeInt(0);
            encoder.writeInt(0);
        };

        /**
         * code: 4
         * name: com.aliyun.dts.deliver.protocol.avro.generated.Float
         */
        valueSerializers[ValueType.FLOAT_NUMERIC.ordinal()] = (encoder, value) -> {
            FloatNumeric numeric = (FloatNumeric) value;

            encoder.writeIndex(4);
            encoder.writeDouble(numeric.getData());
            encoder.writeInt(8);
            encoder.writeInt(15);
        };

        /**
         * code: 5
         * name: com.aliyun.dts.deliver.protocol.avro.generated.Timestamp
         */
        valueSerializers[ValueType.UNIX_TIMESTAMP.ordinal()] = (encoder, value) -> {
            UnixTimestamp unixTimestamp = (UnixTimestamp) value;

            encoder.writeIndex(5);
            encoder.writeLong(unixTimestamp.getTimestampSec());
            Integer micro = unixTimestamp.getMicro();
            if (null == micro) {
                encoder.writeInt(0);
            } else {
                encoder.writeInt(micro);
            }
        };

        /**
         * code: 6
         * name: com.aliyun.dts.deliver.protocol.avro.generated.DateTime
         */
        /**
         * code: 7
         * name: com.aliyun.dts.deliver.protocol.avro.generated.TimestampWithTimeZone
         */
        valueSerializers[ValueType.DATETIME.ordinal()] = (encoder, value) -> {

            DateTime dateTime = (DateTime) value;
            if (dateTime.isSet(DateTime.SEG_TIMEZONE)) {
                encoder.writeIndex(7);
                serializeDateTime(encoder, dateTime);
                encoder.writeString(dateTime.getTimeOffset(), true);
            } else {
                encoder.writeIndex(6);
                serializeDateTime(encoder, dateTime);
            }
        };

        /**
         * code: 8
         * name: com.aliyun.dts.deliver.protocol.avro.generated.BinaryGeometry
         */
        valueSerializers[ValueType.WKB_GEOMETRY.ordinal()] = (encoder, value) -> {
            WKBGeometry wkbGeometry = (WKBGeometry) value;

            encoder.writeIndex(8);
            encoder.writeString("WKB", true);
            encoder.writeBytes(wkbGeometry.getData());
        };

        /**
         * code: 9
         * name: com.aliyun.dts.deliver.protocol.avro.generated.TextGeometry
         */
        valueSerializers[ValueType.WKT_GEOMETRY.ordinal()] = (encoder, value) -> {
            WKTGeometry wktGeometry = (WKTGeometry) value;

            encoder.writeIndex(9);
            encoder.writeString("WKT", true);
            encoder.writeString(wktGeometry.getData());
        };

        /**
         * code: 10
         * name: com.aliyun.dts.deliver.protocol.avro.generated.BinaryObject
         */
        valueSerializers[ValueType.BINARY_ENCODING_OBJECT.ordinal()] = (encoder, value) -> {

            BinaryEncodingObject binaryEncodingObject = (BinaryEncodingObject) value;

            encoder.writeIndex(10);
            encoder.writeString(binaryEncodingObject.getObjectType().name(), true);
            encoder.writeBytes(binaryEncodingObject.getData());
        };

        valueSerializers[ValueType.BIT.ordinal()] = (encoder, value) -> {

            BitValue bitValue = (BitValue) value;

            encoder.writeIndex(10);
            encoder.writeString(bitValue.getType().name(), true);
            encoder.writeBytes(bitValue.getData());
        };

        /**
         * code: 11
         * name: com.aliyun.dts.deliver.protocol.avro.generated.TextObject
         */
        valueSerializers[ValueType.TEXT_ENCODING_OBJECT.ordinal()] = (encoder, value) -> {
            TextEncodingObject textEncodingObject = (TextEncodingObject) value;
            encoder.writeIndex(11);
            encoder.writeString(textEncodingObject.getObjectType().name(), true);
            encoder.writeString(textEncodingObject.getData());
        };

        /**
         * code: 12
         * name: com.aliyun.dts.deliver.protocol.avro.generated.EmptyObject
         */
        valueSerializers[ValueType.NONE.ordinal()] = (encoder, value) -> {
            encoder.writeIndex(12);
            encoder.writeEnum(EmptyObject.NONE.ordinal());
        };
    }

    /**
     * 普通字段类型
     *
     * @param recordFields
     * @param rowImage
     * @param encoder
     * @throws IOException
     */
    protected void serializeRowImage(List<RecordField> recordFields, RowImage rowImage, CachedBufferedBinaryEncoder encoder) throws IOException {
        if (null == rowImage || null == rowImage.getValues()) {
            encoder.writeIndex(0);
            encoder.writeNull();
        } else {
            encoder.writeIndex(2);
            Value[] values = rowImage.getValues();
            if (recordFields.size() != values.length) {
                throw new RuntimeException("serialize row image error, record field size not equals value size");
            }
            encoder.writeArrayStart();
            encoder.setItemCount(values.length);
            for (RecordField field : recordFields) {
                encoder.startItem();
                Value value = values[field.getFieldPosition()];
                if (null == value) {
                    encoder.writeIndex(0);
                    encoder.writeNull();
                } else {
                    serializeValue(value, encoder);
                }
            }
            encoder.writeArrayEnd();
        }
    }

    protected void serializeValue(Value value, CachedBufferedBinaryEncoder encoder) throws IOException {
        valueSerializers[value.getType().ordinal()].serialize(encoder, value);
    }

    /**
     * DDL 值
     *
     * @param rowImage
     * @param encoder
     * @throws IOException
     */
    protected void serializeDDLImage(List<RecordField> recordFields, RowImage rowImage, CachedBufferedBinaryEncoder encoder) throws IOException {
        if (null == rowImage
                || null == rowImage.getValues()
                || 0 >= rowImage.getValues().length
                || null == rowImage.getValue(0)) {
            encoder.writeIndex(0);
            encoder.writeNull();
        } else {
            encoder.writeIndex(1);
            encoder.writeString(rowImage.getValue(0).toString());
        }
    }
}
