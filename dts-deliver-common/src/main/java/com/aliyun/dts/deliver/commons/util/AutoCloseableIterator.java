package com.aliyun.dts.deliver.commons.util;

import java.util.Iterator;

/**
 * If you operate on this iterator, you better close it. {@link AutoCloseableIterator#close} must be
 * idempotent. The contract on this interface is that it may be called MANY times.
 *
 * @param <T> type
 */
public interface AutoCloseableIterator<T> extends Iterator<T>, AutoCloseable {}