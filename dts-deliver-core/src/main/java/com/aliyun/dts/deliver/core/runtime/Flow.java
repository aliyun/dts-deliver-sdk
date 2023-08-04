package com.aliyun.dts.deliver.core.runtime;

public interface Flow<T> {

    boolean isIdle();

    void stop(boolean force);

    Throwable getError();

    void clearError();

    boolean shouldLongSleep();

    default String getName() {
        return this.getClass().getSimpleName();
    }
}

