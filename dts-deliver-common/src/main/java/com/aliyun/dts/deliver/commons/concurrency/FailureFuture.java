package com.aliyun.dts.deliver.commons.concurrency;

import java.security.InvalidParameterException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class FailureFuture<T> implements Future<T> {

    private Throwable cause;

    public FailureFuture(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() throws ExecutionException {
        throw new ExecutionException(cause);
    }

    @Override
    public Throwable cause() {
        return cause;
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
