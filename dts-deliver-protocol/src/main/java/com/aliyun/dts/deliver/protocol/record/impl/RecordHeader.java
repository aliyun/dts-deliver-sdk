package com.aliyun.dts.deliver.protocol.record.impl;

public class RecordHeader {

    // 这个数据结构本身的版本
    private int version;

    // 这条记录的唯一标志号
    private long id;

    // 源端的时间戳（数据产生时间）
    private long sourceTimestamp;

    // 源端的位点
    private String sourcePosition;

    // 本条记录所对应的源端安全位点
    private String safeSourcePosition;

    // 本条记录所在的事务的ID
    private String sourceTxid;

    // 源数据库类型
    private String source;

    // 标记来源于源端哪个Source
    private String uniqueSourceName;

    public RecordHeader(String uniqueSourceName, String safeSourcePosition) {
        this.uniqueSourceName = uniqueSourceName;
        this.safeSourcePosition = safeSourcePosition;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSourceTimestamp() {
        return sourceTimestamp;
    }

    public void setSourceTimestamp(long sourceTimestamp) {
        this.sourceTimestamp = sourceTimestamp;
    }

    public String getSourcePosition() {
        return sourcePosition;
    }

    public void setSourcePosition(String sourcePosition) {
        this.sourcePosition = sourcePosition;
    }

    public String getSafeSourcePosition() {
        return safeSourcePosition;
    }

    public void setSafeSourcePosition(String safeSourcePosition) {
        this.safeSourcePosition = safeSourcePosition;
    }

    public String getSourceTxid() {
        return sourceTxid;
    }

    public void setSourceTxid(String sourceTxid) {
        this.sourceTxid = sourceTxid;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getUniqueSourceName() {
        return uniqueSourceName;
    }

    public void setUniqueSourceName(String uniqueSourceName) {
        this.uniqueSourceName = uniqueSourceName;
    }

    @Override
    public String toString() {
        return "RecordHeader{" +
                "version=" + version +
                ", id=" + id +
                ", sourceTimestamp=" + sourceTimestamp +
                ", sourcePosition='" + sourcePosition + '\'' +
                ", safeSourcePosition='" + safeSourcePosition + '\'' +
                ", sourceTxid='" + sourceTxid + '\'' +
                ", source='" + source + '\'' +
                ", uniqueSourceName='" + uniqueSourceName + '\'' +
                '}';
    }

    public long size() {
        // version + id + sourceTimestamp + sourcePosition + safeSourcePosition + sourceTxid + source
        return 4L + 8L + 8L + (null == sourcePosition ? 0L : sourcePosition.getBytes().length)
                + (null == safeSourcePosition ? 0L : safeSourcePosition.getBytes().length)
                + (null == sourceTxid ? 0L : sourceTxid.getBytes().length)
                + (null == source ? 0L : source.getBytes().length);
    }
}
