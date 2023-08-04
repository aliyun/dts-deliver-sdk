package com.aliyun.dts.deliver.commons.functional;

import com.aliyun.dts.deliver.commons.concurrency.Future;

@FunctionalInterface
public interface CheckedConsumer<T, E extends Throwable> {

    Future<Void> accept(T t) throws E;

}