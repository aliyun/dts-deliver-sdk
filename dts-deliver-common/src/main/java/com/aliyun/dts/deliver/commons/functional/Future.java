package com.aliyun.dts.deliver.commons.functional;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public interface Future<T> {

    T get() throws InterruptedException, ExecutionException;

    Throwable cause();

    boolean isDone();

    Future<T> addListener(Consumer<Future<T>> l);

    void success(T result);

    void fail(Throwable cause);
}
