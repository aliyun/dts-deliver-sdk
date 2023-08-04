package com.aliyun.dts.deliver.protocol.record.value;

import java.math.BigDecimal;

public class DecimalNumeric implements Value<BigDecimal> {

    private BigDecimal data;

    public DecimalNumeric() {
    }

    public DecimalNumeric(BigDecimal data) {
        this.data = data;
    }

    public DecimalNumeric(String data) {
        if (null == data) {
            return;
        }
        this.data = new BigDecimal(data);
    }

    @Override
    public ValueType getType() {
        return ValueType.DECIMAL_NUMERIC;
    }

    @Override
    public BigDecimal getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public String toString() {
        if (null == this.data) {
            return null;
        }
        return this.data.toString();
    }

    @Override
    public long size() {
        if (null != data) {
            return data.toBigInteger().toByteArray().length;
        }
        return 0L;
    }

    @Override
    public DecimalNumeric parse(Object rawData) {
        if (null == rawData) {
            return null;
        }
        return new DecimalNumeric(rawData.toString());
    }

    @Override
    public int compareTo(Value value) {
        if (null == value) {
            return 1;
        }
        switch (value.getType()) {
            case WKB_GEOMETRY:
            case BINARY_ENCODING_OBJECT:
            case BIT:
                throw new UnsupportedOperationException("unsupported compare type for(decimal," + value.getType() + ")");
            default:
                return this.data.compareTo(new BigDecimal(value.toString()));
        }
    }
}
