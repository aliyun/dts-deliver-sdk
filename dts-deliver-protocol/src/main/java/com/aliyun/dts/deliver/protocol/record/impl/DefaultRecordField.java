package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.commons.functional.TriFunction;
import com.aliyun.dts.deliver.commons.util.NullableOptional;
import com.aliyun.dts.deliver.protocol.record.RawDataType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.RecordField;
import com.aliyun.dts.deliver.protocol.record.value.Value;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public final class DefaultRecordField implements RecordField {

    public static final boolean DEFAULT_NULLABLE = true;

    private String fieldName;
    private RawDataType rawDataType;
    private RawDataType sourceRawDataType;
    private int originalColumnTypeNumber;
    private String originalColumnTypeName;
    // seems not compatible with source and sink name map, use optional is more flexible
    private Set<String> aliases;
    private Value defaultValue;
    private boolean nullable;
    private boolean unique;
    private boolean primary;
    private boolean indexed;
    private int scale;
    private int precision;
    private boolean autoIncrement = false;
    private boolean generated = false;
    private int fieldPosition;
    private int displaySize;
    private int keySeq;
    private boolean readOnly;
    private String encoding;
    private TriFunction<DefaultRecordField, Record, Boolean, NullableOptional<Value>> valueSupplier;

    private DefaultRecordField() {
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public Set<String> getAliases() {
        return aliases;
    }

    @Override
    public RawDataType getRawDataType() {
        return rawDataType;
    }

    @Override
    public RawDataType getSourceRawDataType() {
        if (null == sourceRawDataType) {
            return getRawDataType();
        }

        return sourceRawDataType;
    }

    @Override
    public void setSourceRawDataType(RawDataType rawDataType) {
        this.sourceRawDataType = rawDataType;
    }

    @Override
    public Value getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public boolean isPrimary() {
        return primary;
    }

    @Override
    public boolean isIndexed() {
        return indexed;
    }

    @Override
    public int keySeq() {
        return keySeq;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Get field position to set/get value, which starts from 0
     */
    @Override
    public int getFieldPosition() {
        return fieldPosition;
    }

    @Override
    public void setFieldPosition(int pos) {
        fieldPosition = pos;
    }

    protected void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setRawDataType(RawDataType dataType) {
        this.rawDataType = dataType;
    }

    @Override
    public int getOriginalColumnTypeNumber() {
        return originalColumnTypeNumber;
    }

    public void setOriginalColumnTypeNumber(int originalColumnTypeNumber) {
        this.originalColumnTypeNumber = originalColumnTypeNumber;
    }

    @Override
    public String getOriginalColumnTypeName() {
        return originalColumnTypeName;
    }

    public void setOriginalColumnTypeName(String originalColumnTypeName) {
        this.originalColumnTypeName = originalColumnTypeName;
    }

    private void setAliases(Set<String> aliases) {
        this.aliases = aliases;
    }

    private void setDefaultValue(Value defaultValue) {
        this.defaultValue = defaultValue;
    }

    public DefaultRecordField setNullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    @Override
    public DefaultRecordField setUnique(boolean unique) {
        this.unique = unique;
        return this;
    }

    public DefaultRecordField setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public DefaultRecordField setIndexed(boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public DefaultRecordField setKeySeq(int keySeq) {
        this.keySeq = keySeq;
        return this;
    }

    //TODO(linyuan) float,double需要依赖这个值进行数据舍入
    @Override
    public int getScale() {
        return this.scale;
    }

    private void setScale(int scale) {
        this.scale = scale;
    }

    private void setPrecision(int precision) {
        this.precision = precision;
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    @Override
    public boolean isAutoIncrement() {
        return this.autoIncrement;
    }

    private void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    @Override
    public boolean isGenerated() {
        return this.generated;
    }

    @Override
    public boolean isDynamic() {
        return null != valueSupplier;
    }

    private void setGenerated(boolean generated) {
        this.generated = generated;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
    }

    public void setDisplaySize(int displaySize) {
        this.displaySize = displaySize;
    }

    @Override
    public void resetAlias(String alias) {
        aliases = Collections.singleton(alias);
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    @Override
    public NullableOptional<Value> generateDynamicValue(Record record, boolean forBeforeImage) {
        if (!isDynamic()) {
            throw new InvalidParameterException("Field " + getFieldName() + " is not dynamic");
        }
        return valueSupplier.apply(this, record, forBeforeImage);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + rawDataType.hashCode();
        result = prime * result + fieldName.hashCode();
        result = prime * result + aliases.hashCode();
        result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
        result = prime * result + Boolean.hashCode(nullable);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        DefaultRecordField other = (DefaultRecordField) obj;
        return rawDataType.equals(other.rawDataType) && fieldName.equals(other.getFieldName()) && aliases.equals(other.getAliases()) && Objects.equals(defaultValue, other.defaultValue)
            && nullable == other.nullable;
    }

    @Override
    public String toString() {
        StringBuilder sbl = new StringBuilder();
        sbl.append(fieldName)
            .append(" ").append(rawDataType.getTypeName())
            .append(" ").append(!nullable ? "not " : "").append("nullable");
        if (isGenerated()) {
            sbl.append(" ").append("generated");
        }
        if (isDynamic()) {
            sbl.append(" ").append("dynamic");
        }
        if (isPrimary()) {
            sbl.append(" ").append("primary");
        }
        if (isUnique()) {
            sbl.append(" ").append("unique");
        }
        if (!aliases.isEmpty()) {
            sbl.append(" ").append("as ");
            boolean first = true;
            for (String name : aliases) {
                if (!first) {
                    sbl.append(",");
                }
                sbl.append(name);
                first = false;
            }
        }
        return sbl.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    //构造函数太多，改成Build模式。同时所有setxxx方法改成private
    public static class Builder {
        private String fieldName;
        private RawDataType rawDataType;
        private RawDataType sourceRawDataType;
        private int originalColumnTypeNumber;
        private String originalColumnTypeName;
        private Set<String> aliases = Collections.emptySet();
        private Value defaultValue;
        private String encoding;
        private boolean nullable = DEFAULT_NULLABLE;
        private boolean unique = false;
        private boolean primary = false;
        private boolean indexed = false;
        private int scale;
        private int precision = -1;
        private boolean autoIncrement = false;
        private boolean generated = false;
        private int fieldPosition = 0;
        private int displaySize;
        private int keySeq;
        private boolean readOnly;
        private TriFunction<DefaultRecordField, Record, Boolean, NullableOptional<Value>> valueSupplier;

        public String name() {
            return this.fieldName;
        }

        public int precision() {
            return precision;
        }

        public RawDataType rawDataType() {
            return rawDataType;
        }

        public Builder withFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder withRawDataType(RawDataType rawDataType) {
            this.rawDataType = rawDataType;
            return this;
        }

        public Builder withSourceRawDataType(RawDataType sourceRawDataType) {
            this.sourceRawDataType = sourceRawDataType;
            return this;
        }

        public Builder withAliases(Set<String> aliases) {
            // If aliases is the empty set, don't bother with the expense of wrapping in an unmodifiableSet.
            Objects.requireNonNull(aliases);
            if (aliases == Collections.EMPTY_SET) {
                this.aliases = aliases;
            } else {
                this.aliases = Collections.unmodifiableSet(aliases);
            }
            this.aliases = aliases;
            return this;
        }

        public Builder withDefaultValue(Value defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder withNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder withUnique(boolean unique) {
            this.unique = unique;
            return this;
        }

        public Builder withPrimary(boolean primary) {
            this.primary = primary;
            return this;
        }

        public Builder withIndexed(boolean indexed) {
            this.indexed = indexed;
            return this;
        }

        public Builder withScale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder withPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder withAutoIncrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder withGenerated(boolean generated) {
            this.generated = generated;
            return this;
        }

        public Builder withDynamic(TriFunction<DefaultRecordField, Record, Boolean, NullableOptional<Value>> valueSupplier) {
            this.valueSupplier = valueSupplier;
            return this;
        }

        public Builder withFieldPosition(int fieldPosition) {
            this.fieldPosition = fieldPosition;
            return this;
        }

        public Builder setDisplaySize(int displaySize) {
            this.displaySize = displaySize;
            return this;
        }

        public Builder withKeySeq(int keySeq) {
            this.keySeq = keySeq;
            return this;
        }

        public Builder withEncoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder withReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }

        public int getOriginalColumnTypeNumber() {
            return originalColumnTypeNumber;
        }

        public Builder setOriginalColumnTypeNumber(int originalColumnTypeNumber) {
            this.originalColumnTypeNumber = originalColumnTypeNumber;
            return this;
        }

        public String getOriginalColumnTypeName() {
            return originalColumnTypeName;
        }

        public Builder setOriginalColumnTypeName(String originalColumnTypeName) {
            this.originalColumnTypeName = originalColumnTypeName;
            return this;
        }

        public DefaultRecordField get() {
            DefaultRecordField recordField = new DefaultRecordField();
            this.fieldName = Objects.requireNonNull(fieldName);
            recordField.setFieldName(fieldName);

            this.rawDataType = Objects.requireNonNull(rawDataType);
            recordField.setRawDataType(rawDataType);
            recordField.setSourceRawDataType(sourceRawDataType);
            recordField.setOriginalColumnTypeName(originalColumnTypeName);
            recordField.setOriginalColumnTypeNumber(originalColumnTypeNumber);
            recordField.setAliases(aliases);
            recordField.setDefaultValue(defaultValue);
            recordField.setNullable(nullable);
            recordField.setPrimary(primary);
            recordField.setUnique(unique);
            recordField.setIndexed(indexed);
            recordField.setScale(scale);
            recordField.setPrecision(precision);
            recordField.setAutoIncrement(autoIncrement);
            recordField.setGenerated(generated);
            recordField.setFieldPosition(fieldPosition);
            recordField.setDisplaySize(displaySize);
            recordField.setKeySeq(keySeq);
            recordField.setReadOnly(readOnly);
            recordField.setEncoding(encoding);
            recordField.valueSupplier = valueSupplier;
            return recordField;
        }
    }
}
