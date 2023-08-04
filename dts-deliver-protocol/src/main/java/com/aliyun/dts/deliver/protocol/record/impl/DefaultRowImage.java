package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.commons.util.NullableOptional;
import com.aliyun.dts.deliver.protocol.record.*;
import com.aliyun.dts.deliver.protocol.record.value.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultRowImage implements RowImage {

    private final RecordSchema recordSchema;
    private final Value[] values;
    private long size = -1;

    public DefaultRowImage(RecordSchema recordSchema) {
        this(recordSchema, recordSchema.getFieldCount());
    }

    public DefaultRowImage(RecordSchema recordSchema, int fieldCount) {
        this.recordSchema = recordSchema;
        this.values = new Value[fieldCount];
    }

    @Override
    public Value[] getValues() {
        return this.values;
    }

    @Override
    public Value getValue(int index) {
        return values[index];
    }

    @Override
    public Value getValue(RecordField field) {
        return getValue(field.getFieldPosition());
    }

    @Override
    public NullableOptional<Value> getValue(String fieldName) {
        return recordSchema.getField(fieldName)
                .map(field -> getValue(field));
    }

    @Override
    public NullableOptional<Value> getValueIgnoreCase(String fieldName) {
        return recordSchema.getFieldIgnoreCase(fieldName)
                .map(field -> getValue(field));
    }

    private void accumulateSize(Value value) {
        if (null != value) {
            size += value.size();
        }
    }

    public void setValue(int i, Value value) {
        values[i] = value;
    }

    public void setValue(String fieldName, Value value) {
        RecordField recordField = recordSchema.getField(fieldName)
                .orElse(null);
        setValue(recordField, value);
    }

    public void setValue(RecordField field, Value value) {
        int index = field.getFieldPosition();
        setValue(index, value);
    }

    @Override
    public Map<String, Value> toMap(Function<String, String> filedNameResolver, Function<Value, Value> valueResolver) {
        Map<String, Value> valueMap = new TreeMap<>();
        int i = 0;

        for (RecordField field : recordSchema.getFields()) {
            valueMap.put(filedNameResolver == null ? field.getFieldName() : filedNameResolver.apply(field.getFieldName()),
                    valueResolver == null ? values[i] : valueResolver.apply(values[i]));
            i++;
        }

        return valueMap;
    }

    @SuppressWarnings("unchecked")
    public Pair<RecordField, Value>[] buildFieldValuePairArray(Collection<RecordField> recordFields) {
        Pair<RecordField, Value>[] rs = new Pair[recordFields.size()];
        int index = 0;
        for (RecordField recordField : recordFields) {
            rs[index] = Pair.of(recordField, getValue(recordField));
            index++;
        }

        return rs;
    }

    @Override
    public Pair<RecordField, Value>[] getPrimaryKeyValues() {
        RecordIndexInfo recordIndexInfo = recordSchema.getPrimaryIndexInfo();
        if (null == recordIndexInfo) {
            return null;
        }

        return buildFieldValuePairArray(recordIndexInfo.getIndexFields());
    }

    private Pair<RecordField, Value>[] buildAllFieldValuePairArray(List<? extends RecordIndexInfo> recordIndexInfoList) {
        if (null == recordIndexInfoList || recordIndexInfoList.isEmpty()) {
            return null;
        }

        Set<RecordField> recordFieldSet = recordIndexInfoList.stream()
                .flatMap(indexInfo -> indexInfo.getIndexFields().stream())
                .collect(Collectors.toSet());

        return buildFieldValuePairArray(recordFieldSet);
    }

    @Override
    public Pair<RecordField, Value>[] getUniqueKeyValues() {
        List<RecordIndexInfo> recordIndexInfoList = recordSchema.getUniqueIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    @Override
    public Pair<RecordField, Value>[] getForeignKeyValues() {
        List<ForeignKeyIndexInfo> recordIndexInfoList = recordSchema.getForeignIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    @Override
    public long size() {
        if (-1 == this.size) {
            this.size = 0;
            if (null != this.values) {
                for (int i = 0; i < this.values.length; ++i) {
                    accumulateSize(this.values[i]);
                }
            }
        }
        return size;
    }

    @Override
    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    @Override
    public String toString() {
        return StringUtils.join(values, ",");
    }
}
