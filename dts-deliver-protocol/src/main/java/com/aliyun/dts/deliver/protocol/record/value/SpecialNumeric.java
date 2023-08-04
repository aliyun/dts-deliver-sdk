package com.aliyun.dts.deliver.protocol.record.value;

import org.apache.commons.lang3.StringUtils;

public class SpecialNumeric implements Value<SpecialNumeric.SpecialNumericType> {

    private static final String NAN = "NaN";
    private static final String INFINITY = "Infinity";
    private static final String NEGATIVE_INFINITY = "-Infinity";
    private static final String NEAR = "~";

    private SpecialNumericType value;

    public SpecialNumeric(SpecialNumericType value) {
        this.value = value;
    }

    public SpecialNumeric(String text) {
        this(SpecialNumericType.parseFrom(text));
    }

    @Override
    public ValueType getType() {
        return ValueType.SPECIAL_NUMERIC;
    }

    @Override
    public SpecialNumericType getData() {
        return this.value;
    }

    @Override
    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public long size() {
        return Integer.BYTES;
    }

    public enum SpecialNumericType {
        NOT_ASSIGNED,
        INFINITY,
        NEGATIVE_INFINITY,
        NOT_A_NUMBER,
        NAN,
        NEAR;

        public static SpecialNumericType parseFrom(String value) {
            if (SpecialNumeric.NAN.equals(value)) {
                return NAN;
            }
            if (SpecialNumeric.NEAR.equals(value)) {
                return NEAR;
            }
            if (SpecialNumeric.INFINITY.equals(value)) {
                return INFINITY;
            }
            if (SpecialNumeric.NEGATIVE_INFINITY.equals(value)) {
                return NEGATIVE_INFINITY;
            }
            return SpecialNumericType.valueOf(value);
        }

        @Override
        public String toString() {
            if (this.equals(NAN)) {
                return SpecialNumeric.NAN;
            }
            if (this.equals(NEAR)) {
                return SpecialNumeric.NEAR;
            }
            if (this.equals(INFINITY)) {
                return SpecialNumeric.INFINITY;
            }
            if (this.equals(NEGATIVE_INFINITY)) {
                return SpecialNumeric.NEGATIVE_INFINITY;
            }
            return this.name();
        }
    }

    @Override
    public SpecialNumeric parse(Object rawData) {
        return new SpecialNumeric(rawData.toString());
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
                throw new UnsupportedOperationException("unsupported compare type for(special numeric," + value.getType() + ")");
            default:
                return StringUtils.compare(this.toString(), value.toString());
        }
    }
}
