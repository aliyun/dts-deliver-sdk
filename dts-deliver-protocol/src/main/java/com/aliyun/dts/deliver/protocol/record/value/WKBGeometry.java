package com.aliyun.dts.deliver.protocol.record.value;

import com.aliyun.dts.deliver.protocol.record.util.BytesUtil;
import com.aliyun.dts.deliver.protocol.record.util.GeometryUtil;

import java.nio.ByteBuffer;

public class WKBGeometry implements Value<ByteBuffer> {

    private ByteBuffer data;

    public WKBGeometry(ByteBuffer data) {
        this.data = data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.WKB_GEOMETRY;
    }

    @Override
    public ByteBuffer getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public long size() {
        if (null != data) {
            return data.capacity();
        }

        return 0L;
    }

    @Override
    public String toString() {
        try {
            return GeometryUtil.getGeometryFromBytes(data.array()).toString();
        } catch (Exception ex) {
            return BytesUtil.byteBufferToHexString(data);
        }
    }

    @Override
    public WKBGeometry parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        if (rawData instanceof byte[]) {
            return new WKBGeometry(ByteBuffer.wrap((byte[]) rawData));
        }

        return new WKBGeometry(BytesUtil.hexStringToByteBuffer(rawData.toString()));
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
                return BytesUtil.compareTo(this.data, (ByteBuffer) value.getData());
            default:
                throw new UnsupportedOperationException("unsupported compare type for(binary," + value.getType() + ")");
        }
    }
}
