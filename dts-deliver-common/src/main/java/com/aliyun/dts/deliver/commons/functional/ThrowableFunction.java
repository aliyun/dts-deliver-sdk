package com.aliyun.dts.deliver.commons.functional;

public interface ThrowableFunction<T> {
    T call() throws Exception;
}
