package com.aliyun.dts.deliver.protocol.record.value;

import com.aliyun.dts.deliver.protocol.record.util.BytesUtil;

import java.nio.ByteBuffer;

public class BinaryEncodingObject implements Value<ByteBuffer> {

    private ObjectType objectType;
    private ByteBuffer binaryData;

    public BinaryEncodingObject(ObjectType objectType, ByteBuffer binaryData) {
        this.objectType = objectType;
        this.binaryData = binaryData;
    }

    @Override
    public ValueType getType() {
        return ValueType.BINARY_ENCODING_OBJECT;
    }

    @Override
    public ByteBuffer getData() {
        return binaryData;
    }

    @Override
    public boolean isNull() {
        return binaryData == null;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    @Override
    public long size() {
        if (null != binaryData) {
            return binaryData.capacity();
        }

        return 0L;
    }

    public String toString() {
        return BytesUtil.byteBufferToHexString(binaryData);
    }

    @Override
    public BinaryEncodingObject parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        ByteBuffer byteBuffer = null;

        if (rawData instanceof String) {
            byteBuffer = BytesUtil.hexStringToByteBuffer(rawData.toString());
        } else if (rawData instanceof byte[]) {
            byteBuffer = ByteBuffer.wrap((byte[]) rawData);
        }

        return new BinaryEncodingObject(objectType, byteBuffer);
    }

    @Override
    public int compareTo(Value value) {
        if (null == value) {
            return 1;
        }
        switch (value.getType()) {
            case BINARY_ENCODING_OBJECT:
            case BIT:
            case WKB_GEOMETRY:
                return BytesUtil.compareTo(this.binaryData, (ByteBuffer) value.getData());
            default:
                throw new UnsupportedOperationException("unsupported compare type for(binary," + value.getType() + ")");
        }
    }
}
