package com.aliyun.dts.deliver.protocol.record.value;

import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.protocol.record.util.BytesUtil;
import com.aliyun.dts.deliver.protocol.record.util.JDKCharsetMapper;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

public class StringValue implements Value<ByteBuffer> {

    public static final String DEFAULT_CHARSET = "UTF-8";
    private ByteBuffer data;
    private String charset;
    private String rawString;
    private static ThreadLocal<Map<String, Charset>> charsetMap = new ThreadLocal<>();

    public StringValue(ByteBuffer data, String charset) {
        this.data = data;
        this.charset = charset;
    }

    public StringValue(String data) {
        this.rawString = data;
        this.charset = DEFAULT_CHARSET;
    }

    public String getCharset() {
        return this.charset;
    }

    @Override
    public ValueType getType() {
        return ValueType.STRING;
    }

    @Override
    public boolean isNull() {
        return data == null && rawString == null;
    }

    @Override
    public ByteBuffer getData() {
        if (this.data != null) {
            return data;
        }
        if (this.rawString != null) {
            this.data = ByteBuffer.wrap(SwallowException.callAndThrowRuntimeException(() -> rawString.getBytes(DEFAULT_CHARSET)));
            this.charset = DEFAULT_CHARSET;
        }
        return this.data;
    }

    @Override
    public String toString() {

        if (rawString != null) {
            return rawString;
        }
        if (data == null) {
            return null;
        }
         // just return hex string if missing charset
        if (StringUtils.isEmpty(charset)) {
            return BytesUtil.byteBufferToHexString(data);
        }

        // try encode data by specified charset
        Map<String, Charset> localMap = charsetMap.get();
        if (null == localMap) {
            localMap = new HashMap<>();
            charsetMap.set(localMap);
        }
        try {
            Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(charset));
            return new String(data.array(), charsetObject);
        } catch (Exception e1) {
            try {
                Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(JDKCharsetMapper.getJDKECharset(charset)));
                return new String(data.array(), charsetObject);
            } catch (Exception e2) {
                return charset + "_'" + BytesUtil.byteBufferToHexString(data) + "'";
            }
        }
    }

    public String toString(String targetCharset) {
        //TODO(huoyu): convert
        return "to impl";
    }

    @Override
    public long size() {
        if (null != data) {
            return data.capacity();
        }

        if (null != rawString) {
            return rawString.length();
        }

        return 0L;
    }

    @Override
    public StringValue parse(Object rawData) {
        if (null == rawData) {
            return null;
        }
        if (rawData instanceof String) {
            return new StringValue(rawData.toString());
        }
        if (rawData instanceof StringValue) {
            return (StringValue) rawData;
        }
        if (rawData instanceof byte[]) {
            return new StringValue(ByteBuffer.wrap((byte[]) rawData), "binary");
        }
        if (rawData instanceof ByteBuffer) {
            return new StringValue((ByteBuffer) rawData, null);
        }
        throw new InvalidParameterException("not support parse raw data with type " + rawData.getClass().getName());
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
                throw new UnsupportedOperationException("unsupported compare type for(integer," + value.getType() + ")");
            default:
                return StringUtils.compare(this.toString(), value.toString());
        }
    }
}
