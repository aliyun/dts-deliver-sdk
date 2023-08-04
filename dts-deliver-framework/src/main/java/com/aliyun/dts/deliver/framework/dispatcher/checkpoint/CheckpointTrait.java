package com.aliyun.dts.deliver.framework.dispatcher.checkpoint;

import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;

public abstract class CheckpointTrait {

    private CheckpointTrait prev;
    private CheckpointTrait next;

    private RecordCheckpoint safeCheckpoint;

    void setPrev(CheckpointTrait prev) {
        this.prev = prev;
    }

    CheckpointTrait getPrev() {
        return prev;
    }

    void setNext(CheckpointTrait next) {
        this.next = next;
    }

    CheckpointTrait getNext() {
        return next;
    }

    public void setSafeCheckpoint(RecordCheckpoint safeCheckpoint) {
        this.safeCheckpoint = safeCheckpoint;
    }

    public RecordCheckpoint getSafeCheckpoint() {
        if (safeCheckpoint != null) {
            return this.safeCheckpoint;
        } else {
            this.safeCheckpoint = computeSafeCheckpoint();
            return this.safeCheckpoint;
        }
    }

    protected abstract RecordCheckpoint computeSafeCheckpoint();

    public abstract Record getHeartbeatRecord();


    //from which source
    public abstract String getUniqueSourceName();
}
