package com.aliyun.dts.deliver.framework.dispatcher.checkpoint;

import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MinimalCheckpointMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(MinimalCheckpointMaintainer.class);
    private Map<String, CheckpointHolder> checkpointHolders = new ConcurrentHashMap<>(16);

    public void initialize(List<Pair<String, RecordCheckpoint>> startCheckpoint) {
        Preconditions.checkNotNull(startCheckpoint);
        startCheckpoint.forEach(pair -> computeCheckpointHolder(pair.getKey(), pair.getValue()));
    }

    public void put(CheckpointTrait checkpoint) {
        String uniqueSourceName = checkpoint.getUniqueSourceName();
        Preconditions.checkState(uniqueSourceName != null);

        Record heartbeatRecord = checkpoint.getHeartbeatRecord();
        if (null == heartbeatRecord) {
            realPutNormalCheckpoint(uniqueSourceName, checkpoint);
        } else {
            realPutHeartbeatRecord(uniqueSourceName, heartbeatRecord);
        }
    }

    private CheckpointHolder computeCheckpointHolder(String source, RecordCheckpoint startCheckpoint) {
        return checkpointHolders.computeIfAbsent(source, (key) -> new CheckpointHolder(startCheckpoint));
    }

    private void realPutNormalCheckpoint(String source, CheckpointTrait checkpoint) {
        CheckpointTrait head = computeCheckpointHolder(source, null).flyingCheckpointHead;
        Preconditions.checkState(head != null);
        CheckpointTrait tail = head.getPrev();
        Preconditions.checkState(tail != null);

        // add current checkpoint to the tail
        tail.setNext(checkpoint);
        checkpoint.setPrev(tail);
        checkpoint.setNext(head);
        head.setPrev(checkpoint);
    }

    private void realPutHeartbeatRecord(String source, Record heartbeatRecord) {
        CheckpointHolder checkpointHolder = computeCheckpointHolder(source, null);
        CheckpointTrait head = checkpointHolder.flyingCheckpointHead;
        Preconditions.checkState(head != null);

        /**
         * if the flying checkpoints belong to sourceName is empty, we can safely update
         * the minimal checkpoint by hb checkpoint.
         */
        if (head.getNext().equals(head)) {
            checkpointHolder.minimalCheckpoint = heartbeatRecord.getRecordCheckpoint();
        }
    }

    /**
     * for example this is a dque: header <-> a <-> b<-> c <-> d <-> header
     * if this traceable is c, we use c's safecheckpoint to set b's safecheckpoint
     * and remove c. By this way, when a,b is removed, the minimalCheckpoints will
     * use c's checkpoint.
     */
    public void remove(CheckpointTrait checkpoint) {
        String source = checkpoint.getUniqueSourceName();
        Preconditions.checkState(source != null);

        CheckpointHolder checkpointHolder = computeCheckpointHolder(source, null);
        CheckpointTrait head = checkpointHolder.flyingCheckpointHead;

        // checkpoint to be removed is the first valid checkpoint
        if (checkpoint.getPrev() == head) {
            if (null != checkpoint.getSafeCheckpoint()) {
                checkpointHolder.minimalCheckpoint = checkpoint.getSafeCheckpoint();
            } else {
                LOG.warn("missing the exact checkpoint here {}", checkpoint);
            }
        } else {
            if (null != checkpoint.getSafeCheckpoint()) {
                checkpoint.getPrev().setSafeCheckpoint(checkpoint.getSafeCheckpoint());
            } else {
                LOG.warn("missing the exact checkpoint here {}", checkpoint);
            }
        }

        // real delete the checkpoint in list
        checkpoint.getPrev().setNext(checkpoint.getNext());
        checkpoint.getNext().setPrev(checkpoint.getPrev());
    }

    public List<Pair<String, RecordCheckpoint>> getMinimalCheckpoint() {
        Long minTimestamp = Long.MAX_VALUE;
        List<Pair<String, RecordCheckpoint>> minCheckpointList = new ArrayList<>();

        for (Map.Entry<String, CheckpointHolder> entry : checkpointHolders.entrySet()) {
            String source = entry.getKey();
            CheckpointHolder checkpointHolder = entry.getValue();
            minCheckpointList.add(Pair.of(source, checkpointHolder.minimalCheckpoint));
        }

        return minCheckpointList;
    }

    static class CheckpointTraitHead extends CheckpointTrait {
        CheckpointTraitHead() {
            setNext(this);
            setPrev(this);
        }

        @Override
        protected RecordCheckpoint computeSafeCheckpoint() {
            return null;
        }

        @Override
        public Record getHeartbeatRecord() {
            return null;
        }

        @Override
        public String getUniqueSourceName() {
            return null;
        }
    }

    static class CheckpointHolder {
        CheckpointTrait flyingCheckpointHead;
        RecordCheckpoint minimalCheckpoint;
        CheckpointHolder(RecordCheckpoint startCheckpoint) {
            flyingCheckpointHead = new CheckpointTraitHead();
            minimalCheckpoint = startCheckpoint;
        }
    }
}
