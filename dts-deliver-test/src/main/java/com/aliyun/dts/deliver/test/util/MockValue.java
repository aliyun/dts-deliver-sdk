package com.aliyun.dts.deliver.test.util;

import com.aliyun.dts.deliver.protocol.record.value.StringValue;
import com.aliyun.dts.deliver.protocol.record.value.Value;
import com.aliyun.dts.deliver.protocol.record.value.ValueType;
import org.apache.commons.lang3.StringUtils;

public class MockValue extends StringValue {
    final String name;
    final String content;
    final String indexName;
    final boolean isPK;
    final boolean isBeforeImage;

    public MockValue(String name, String content, String indexName, boolean isPK, boolean isBeforeImage) {
        super(content);
        this.name = name;
        this.content = content;
        this.indexName = indexName;
        this.isPK = isPK;
        this.isBeforeImage = isBeforeImage;
    }

    @Override
    public ValueType getType() {
        return ValueType.STRING;
    }

//    @Override
//    public String getData() {
//        return content;
//    }

    @Override
    public String toString() {
        return content == null ? null : content;
    }

    @Override
    public long size() {
        return StringUtils.length(content);
    }

    public boolean isPK() {
        return isPK;
    }

    public boolean isBeforeImage() {
        return isBeforeImage;
    }

    public String getName() {
        return name;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public int compareTo(Value o) {
        return 0;
    }
}
