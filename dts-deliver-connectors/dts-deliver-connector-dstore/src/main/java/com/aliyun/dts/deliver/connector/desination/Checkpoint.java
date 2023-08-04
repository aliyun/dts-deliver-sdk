package com.aliyun.dts.deliver.connector.desination;

public class Checkpoint {
    private long offset;
    private String checkpoint;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Override
    public String toString() {
        return "Checkpoint{"
                + "offset=" + offset
                + ", checkpoint='" + checkpoint + '\''
                + '}';
    }
}
