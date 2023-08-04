package com.aliyun.dts.deliver.protocol.record.value;

import org.apache.commons.lang3.StringUtils;

public class WKTGeometry implements Value<String> {

    private long srid;
    private String data;

    public WKTGeometry(String data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.WKT_GEOMETRY;
    }

    @Override
    public String getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public long size() {
        if (null != data) {
            return StringUtils.length(data);
        }

        return 0L;
    }

    @Override
    public String toString() {
        return data;
    }

    @Override
    public WKTGeometry parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        return new WKTGeometry(rawData.toString());
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
                throw new UnsupportedOperationException("unsupported compare type for(integer," + value.getType() + ")");
            default:
                return StringUtils.compare(this.toString(), value.toString());
        }
    }
}
