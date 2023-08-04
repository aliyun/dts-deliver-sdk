package com.aliyun.dts.deliver.core.runtime.standalone;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.functional.ThrowableFunction;
import com.aliyun.dts.deliver.commons.functional.ThrowableFunctionVoid;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public class DefaultDtsContext implements DtsContext {
    @Override
    public void retry(ThrowableFunctionVoid throwableFunctionVoid, BiFunction<Throwable, Integer, Boolean> recoverableChecker) throws Exception {

    }

    @Override
    public <T> T retry(ThrowableFunction<T> throwableFunction, BiFunction<Throwable, Integer, Boolean> recoverableChecker) throws Exception {
        return null;
    }

    @Override
    public void addSource(String uniqueSource) {

    }

    @Override
    public Set<String> getUniqueSources() {
        return null;
    }

    @Override
    public void addStartCheckpoints(String source, RecordCheckpoint recordCheckpoint) {

    }

    @Override
    public List<Pair<String, RecordCheckpoint>> getStartCheckpoints() {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStopped() {
        return false;
    }
}
