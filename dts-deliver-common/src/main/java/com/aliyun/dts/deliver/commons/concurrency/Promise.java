package com.aliyun.dts.deliver.commons.concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class Promise<T> implements Future<T> {

    private Consumer<Future<T>> listener;
    protected volatile T result;
    protected volatile Throwable cause;
    private CountDownLatch resultLatch = new CountDownLatch(1);

    public Future<T> addListener(Consumer<Future<T>> l) {
        if (this.isDone()) {
            l.accept(this);
            return this;
        }

        synchronized (this) {
            if (this.isDone()) {
                l.accept(this);
                return this;
            }

            if (null == listener) {
                this.listener = l;
            } else {
                this.listener = this.listener.andThen(l);
            }
        }

        return this;
    }

    public boolean isDone() {
        return resultLatch.getCount() == 0;
    }

    public T get() throws InterruptedException, ExecutionException {
        resultLatch.await();

        if (null != cause) {
            throw new ExecutionException(cause);
        }

        return result;
    }

    public Throwable cause() {
        return cause;
    }

    private void notifyListener() {
        synchronized (this) {
            if (null != listener) {
                listener.accept(this);
            }
        }
    }

    public void success(T result) {
        this.result = result;
        this.cause = null;

        resultLatch.countDown();

        notifyListener();
    }

    public void fail(Throwable cause) {
        this.cause = cause;
        this.result = null;

        resultLatch.countDown();

        notifyListener();
    }
}
