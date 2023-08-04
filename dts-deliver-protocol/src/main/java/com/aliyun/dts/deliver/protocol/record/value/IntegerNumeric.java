package com.aliyun.dts.deliver.protocol.record.value;

import java.math.BigDecimal;
import java.math.BigInteger;

public class IntegerNumeric implements Value<BigInteger> {

    private BigInteger data;
    private String dataString;

    public IntegerNumeric() {
    }

    public IntegerNumeric(long value) {
        data = BigInteger.valueOf(value);
        dataString = String.valueOf(value);
    }

    public IntegerNumeric(BigInteger value) {
        this.data = value;
    }

    public IntegerNumeric(String value) {
        this.data = new BigInteger(value);
        this.dataString = value;
    }

    @Override
    public ValueType getType() {
        return ValueType.INTEGER_NUMERIC;
    }

    @Override
    public BigInteger getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public String toString() {
        if (dataString != null) {
            return dataString;
        }
        this.dataString = this.data.toString();
        return dataString;
    }

    @Override
    public long size() {
        if (null != data) {
            return data.toByteArray().length;
        }

        return 0L;
    }

    @Override
    public IntegerNumeric parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        return new IntegerNumeric(rawData.toString());
    }

    @Override
    public int compareTo(Value value) {
        if (null == value) {
            return 1;
        }
        switch (value.getType()) {
            case INTEGER_NUMERIC:
            case DECIMAL_NUMERIC:
            case FLOAT_NUMERIC:
            case STRING:
            case TEXT_ENCODING_OBJECT:
                return new BigDecimal(this.data).compareTo(new BigDecimal(value.toString()));
            case UNIX_TIMESTAMP:
                return new BigDecimal(this.data).compareTo(new BigDecimal(((UnixTimestamp) value).getTimestampSec()));
            default:
                throw new UnsupportedOperationException("unsupported compare type for(integer," + value.getType() + ")");
        }
    }
}
