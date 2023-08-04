package com.aliyun.dts.deliver.framework.dispatcher.checkpoint;

import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

public class CheckpointManager {
    private MinimalCheckpointMaintainer checkpoints = new MinimalCheckpointMaintainer();

    public CheckpointManager(List<Pair<String, RecordCheckpoint>> startCheckpoints) {
        checkpoints.initialize(startCheckpoints);
    }

    public synchronized void start(CheckpointTrait checkpoint) {
        checkpoints.put(checkpoint);
    }

    public synchronized void finish(CheckpointTrait checkpoint) {
        checkpoints.remove(checkpoint);
    }

    public synchronized List<Pair<String, RecordCheckpoint>> getMinimalCheckpoints() {
        List<Pair<String, RecordCheckpoint>> minCheckpointList = checkpoints.getMinimalCheckpoint();

        return minCheckpointList;
    }
}
