package com.aliyun.dts.deliver.protocol.record.value;

public class InMemoryObject implements Value<Object> {

    private ObjectType objectType;
    private Object data;
    private long size;

    public InMemoryObject(ObjectType objectType, Object data) {
        this(objectType, data, 0);
    }

    public InMemoryObject(ObjectType objectType, Object data, long size) {
        this.objectType = objectType;
        this.data = data;
        this.size = size;
    }

    @Override
    public ValueType getType() {
        return ValueType.NONE_ENCODING_OBJECT;
    }

    @Override
    public Object getData() {
        return this.data;
    }

    @Override
    public boolean isNull() {
        return this.data == null;
    }

    @Override
    public long size() {
        return this.size;
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public String toString() {
        return data.toString();
    }

    @Override
    public InMemoryObject parse(Object rawData) {
        return new InMemoryObject(objectType, rawData);
    }

    @Override
    public int compareTo(Value value) {
        throw new UnsupportedOperationException("unsupported function NoneEncodingObject.compareTo()");
    }
}
