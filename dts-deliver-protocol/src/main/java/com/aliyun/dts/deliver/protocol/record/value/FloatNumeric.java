package com.aliyun.dts.deliver.protocol.record.value;

import java.math.BigDecimal;

public class FloatNumeric implements Value<Double> {

    private Double data;

    public FloatNumeric(Double data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.FLOAT_NUMERIC;
    }

    @Override
    public Double getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public String toString() {
        return Double.toString(this.data);
    }

    @Override
    public long size() {
        return Double.BYTES;
    }

    @Override
    public FloatNumeric parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        return new FloatNumeric(Double.parseDouble(rawData.toString()));
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
            case DATETIME:
                throw new UnsupportedOperationException("unsupported compare type for(float," + value.getType() + ")");
            default:
                return new BigDecimal(this.data).compareTo(new BigDecimal(value.toString()));
        }
    }
}
