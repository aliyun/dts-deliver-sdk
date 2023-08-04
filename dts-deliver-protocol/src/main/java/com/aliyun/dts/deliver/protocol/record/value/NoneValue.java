package com.aliyun.dts.deliver.protocol.record.value;

/**
 * 占位字段,不具有任何意义
 */
public class NoneValue implements Value<Boolean> {

    @Override
    public ValueType getType() {
        return ValueType.NONE;
    }

    @Override
    public Boolean getData() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public long size() {
        return 0L;
    }

    @Override
    public NoneValue parse(Object rawData)  {
        if (null == rawData) {
            return null;
        }

        return new NoneValue();
    }

    @Override
    public int compareTo(Value value) {
        throw new UnsupportedOperationException("unsupported function NoneValue.compareTo()");
    }
}
