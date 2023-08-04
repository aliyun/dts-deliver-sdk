package com.aliyun.dts.deliver.protocol.record.value;

import java.security.InvalidParameterException;

public interface Value<T> extends Comparable<Value> {

    /**
     * 获取Value定义类型
     */
    ValueType getType();

    /**
     * Get the internal data of current value.
     */
    T getData();

    default boolean isNull() {
        return false;
    }

    /**
     * Convert current to string by utf-8 encoding.
     */
    String toString();

    /**
     * Get the size of current value.
     */
    long size();

    /**
     * Parse to a new object according to @rawData.
     * @param rawData the data that presents Value<T>, which can only be recognized by this Value type.
     */
    default Value<T> parse(Object rawData) {
        throw new InvalidParameterException("not implement it");
    }

    /**
     * Get rawData is compatible for Value<T>
     * @param rawData the data that presents Value<T>, which can only be recognized by this Value type.
     * @return
     */
    default boolean isCompatible(Object rawData) {
        return true;
    }
}
