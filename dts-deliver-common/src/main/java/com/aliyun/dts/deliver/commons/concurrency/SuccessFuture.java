package com.aliyun.dts.deliver.commons.concurrency;

import java.security.InvalidParameterException;
import java.util.function.Consumer;

public class SuccessFuture<T> implements Future<T> {

    private T result;

    public SuccessFuture(T result) {
        this.result = result;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() {
        return result;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public void success(T result) {
        throw new InvalidParameterException();
    }

    @Override
    public void fail(Throwable cause) {
        throw new InvalidParameterException();
    }

    @Override
    public Future<T> addListener(Consumer<Future<T>> l) {
        l.accept(this);
        return this;
    }
}
