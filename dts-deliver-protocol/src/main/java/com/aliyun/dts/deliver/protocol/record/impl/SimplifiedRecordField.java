package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.commons.util.NullableOptional;
import com.aliyun.dts.deliver.protocol.record.RawDataType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordField;
import com.aliyun.dts.deliver.protocol.record.value.Value;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.Set;

public class SimplifiedRecordField implements RecordField {

    private String fieldName;
    private final RawDataType rawDataType;
    private boolean isPrimaryKey;
    private boolean isUniqueKey;
    private int originalColumnTypeNum;

    private int fieldPosition;

    public SimplifiedRecordField(String fieldName, RawDataType rawDataType) {
        this.fieldName = fieldName;
        this.rawDataType = rawDataType;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Set<String> getAliases() {
        return Collections.emptySet();
    }

    @Override
    public RawDataType getRawDataType() {
        return rawDataType;
    }

    @Override
    public RawDataType getSourceRawDataType() {
        return rawDataType;
    }

    @Override
    public void setSourceRawDataType(RawDataType rawDataType) {
        throw new InvalidParameterException("does not support this function");
    }

    @Override
    public Value getDefaultValue() {
        return null;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public boolean isUnique() {
        return isUniqueKey;
    }

    @Override
    public RecordField setUnique(boolean isUnique) {
        isUniqueKey = isUnique;
        return this;
    }

    @Override
    public boolean isPrimary() {
        return isPrimaryKey;
    }

    public boolean setPrimary(boolean isPrimary) {
        isPrimaryKey = isPrimary;
        return isPrimaryKey;
    }

    @Override
    public boolean isIndexed() {
        return isPrimaryKey || isUniqueKey;
    }

    @Override
    public boolean isAutoIncrement() {
        return false;
    }

    @Override
    public int keySeq() {
        return 0;
    }

    @Override
    public int getFieldPosition() {
        return fieldPosition;
    }

    @Override
    public void setFieldPosition(int fieldPosition) {
        this.fieldPosition = fieldPosition;
    }

    @Override
    public int getDisplaySize() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    public void setOriginalColumnTypeNum(int originalColumnTypeNum) {
        this.originalColumnTypeNum = originalColumnTypeNum;
    }

    @Override
    public int getOriginalColumnTypeNumber() {
        return originalColumnTypeNum;
    }

    @Override
    public boolean isDynamic() {
        return false;
    }

    @Override
    public NullableOptional<Value> generateDynamicValue(Record record, boolean forBeforeImage) {
        return NullableOptional.empty();
    }
}
