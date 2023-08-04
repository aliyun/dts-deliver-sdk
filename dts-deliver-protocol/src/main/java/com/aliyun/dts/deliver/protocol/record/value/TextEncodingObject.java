package com.aliyun.dts.deliver.protocol.record.value;

import org.apache.commons.lang3.StringUtils;

public class TextEncodingObject implements Value<String> {

    private ObjectType objectType;
    private String data;

    public TextEncodingObject(ObjectType objectType, String data) {
        this.objectType = objectType;
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.TEXT_ENCODING_OBJECT;
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
        return StringUtils.length(data);
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public String toString() {
        return  data;
    }

    @Override
    public TextEncodingObject parse(Object rawData) {
        String data = null == rawData ? null : rawData.toString();
        return new TextEncodingObject(objectType, data);
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
                throw new UnsupportedOperationException("unsupported compare type for(text," + value.getType() + ")");
            default:
                return StringUtils.compare(this.toString(), value.toString());
        }
    }
}
