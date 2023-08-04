package com.aliyun.dts.deliver.protocol.record.value;

import com.aliyun.dts.deliver.protocol.record.util.BytesUtil;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;

public class BitValue implements Value<ByteBuffer> {
    private ByteBuffer value;

    public BitValue() {
    }

    public BitValue(byte[] value) {
        this.value = ByteBuffer.wrap(value);
    }

    public BitValue(ByteBuffer value) {
        this.value = value;
    }

    @Override
    public ValueType getType() {
        return ValueType.BIT;
    }

    @Override
    public ByteBuffer getData() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String toString() {
        try {
            if (value == null) {
                return StringUtils.EMPTY;
            }
            return BytesUtil.bytesToHexString(value.array());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() {
        if (null != value) {
            return value.capacity();
        }

        return 0L;
    }

    @Override
    public BitValue parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        if (rawData instanceof byte[]) {
            return new BitValue(ByteBuffer.wrap((byte[]) rawData));
        }

        return new BitValue(BytesUtil.hexStringToByteBuffer(rawData.toString()));
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
                return BytesUtil.compareTo(this.value, (ByteBuffer) value.getData());
            default:
                throw new UnsupportedOperationException("unsupported compare type for(binary," + value.getType() + ")");
        }
    }
}
