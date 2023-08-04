package com.aliyun.dts.deliver.protocol.record.value;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.DateFormat;

/**
 * 毫秒位可能存在
 */
public class UnixTimestamp implements Value<String> {

    private long timestampSec;
    private Integer micro;

    public UnixTimestamp() {
        this(0L, null);
    }

    public UnixTimestamp(long timestampSec, Integer micro) {
        this.timestampSec = timestampSec;
        this.micro = micro;
    }

    public void setTimestampSec(long second) {
        this.timestampSec = second;
    }

    public long getTimestampSec() {
        return this.timestampSec;
    }

    public void setMicro(Integer micro) {
        this.micro = micro;
    }

    public Integer getMicro() {
        return this.micro;
    }

    @Override
    public ValueType getType() {
        return ValueType.UNIX_TIMESTAMP;
    }

    @Override
    public String getData() {
        return toString();
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return toString(null);
    }

    public String toString(DateFormat dateFormat) {
        Timestamp timestamp = toJdbcTimestamp();
        if (null == dateFormat) {
            return timestamp.toString();
        } else {
            return dateFormat.format(timestamp);
        }
    }

    public Timestamp toJdbcTimestamp() {
        Timestamp timestamp = new Timestamp(this.timestampSec * 1000);
        if (null != this.micro) {
            timestamp.setNanos(this.micro * 1000);
        }
        return timestamp;
    }

    @Override
    public long size() {
        return Long.BYTES + Integer.BYTES;
    }

    @Override
    public UnixTimestamp parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        final String timestampString = rawData.toString();
        int dotIndex = StringUtils.indexOf(timestampString, ".");

        UnixTimestamp rs = new UnixTimestamp();

        if (dotIndex > 0) {
            rs.setMicro(Integer.parseInt(StringUtils.substring(timestampString, dotIndex + 1)));
        } else {
            dotIndex = timestampString.length();
        }

        rs.setTimestampSec(Long.parseLong(StringUtils.substring(timestampString, 0, dotIndex)));

        return rs;
    }

    @Override
    public boolean isCompatible(Object rawData) {
        if (null == rawData) {
            return true;
        } else {
            return rawData instanceof StringValue || rawData instanceof String || rawData instanceof TextEncodingObject;
        }
    }

    @Override
    public int compareTo(Value value) {
        if (null == value) {
            return 1;
        }
        switch (value.getType()) {
            case WKB_GEOMETRY:
            case BINARY_ENCODING_OBJECT:
            case BIT:
                throw new UnsupportedOperationException("unsupported compare type for(unix timestamp," + value.getType() + ")");
            default:
                return StringUtils.compare(this.toString(), value.toString());
        }
    }
}
