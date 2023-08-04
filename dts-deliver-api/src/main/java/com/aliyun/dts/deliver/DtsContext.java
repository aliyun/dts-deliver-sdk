package com.aliyun.dts.deliver;

import com.aliyun.dts.deliver.commons.functional.ThrowableFunction;
import com.aliyun.dts.deliver.commons.functional.ThrowableFunctionVoid;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public interface DtsContext {
    /**
     * Call function with retry mechanism, the return value of function is void.
     */
    void retry(ThrowableFunctionVoid throwableFunctionVoid, BiFunction<Throwable, Integer, Boolean> recoverableChecker) throws Exception;

    /**
     * Call function with retry mechanism, the return value of function is T.
     */
    <T> T retry(ThrowableFunction<T> throwableFunction, BiFunction<Throwable, Integer, Boolean> recoverableChecker) throws Exception;

    /**
     * put unique source to context.
     */
    void addSource(String uniqueSource);

    /**
     * contains all the sources, needs unique.
     */
    Set<String> getUniqueSources();

    /**
     *
     * add source start checkpoint
     */
    void addStartCheckpoints(String source, RecordCheckpoint recordCheckpoint);

    /**
     *
     * @return each source start checkpoint
     */
    List<Pair<String, RecordCheckpoint>>  getStartCheckpoints();

    void stop();

    boolean isStopped();
}
