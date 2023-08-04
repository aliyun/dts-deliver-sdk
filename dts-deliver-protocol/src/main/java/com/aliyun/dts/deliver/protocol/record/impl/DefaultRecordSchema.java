package com.aliyun.dts.deliver.protocol.record.impl;

import com.aliyun.dts.deliver.commons.util.NullableOptional;
import com.aliyun.dts.deliver.commons.util.PrettyOutput;
import com.aliyun.dts.deliver.protocol.record.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultRecordSchema implements RecordSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRecordSchema.class);

    protected final List<RecordField> recordFields;
    protected final Map<String, RecordField> nameIndex;
    protected final Map<String, RecordField> ignoreCaseNameIndex;
    private final String schemaId;
    private final String databaseName;
    private final String tableName;
    private String fullQualifiedName;
    private String charset;

    protected RecordIndexInfo primaryIndexInfo;
    protected List<RecordIndexInfo> uniqueIndexInfo;
    private List<ForeignKeyIndexInfo> foreignIndexInfo;
    protected List<RecordIndexInfo> normalIndexInfo;
    private DatabaseInfo databaseInfo;
    private long totalRows;
    private String condition;
    private List<RecordField> partitionFields;

    private Map<String, RecordField> ignoreRecordFields;

    public DefaultRecordSchema(String schemaId, String databaseName, String tableName, List<RecordField> recordFields) {
        if (null == recordFields) {
            recordFields = Collections.emptyList();
        }

        this.schemaId = schemaId;
        this.databaseName = databaseName;
        this.tableName = tableName;

        this.recordFields = new ArrayList<>(recordFields.size());
        this.recordFields.addAll(recordFields);

        this.nameIndex = new TreeMap<>();
        this.ignoreCaseNameIndex = new TreeMap<>();

        this.recordFields.forEach(
            recordField -> this.nameIndex.put(recordField.getFieldName(), recordField));

        this.primaryIndexInfo = null;
        this.uniqueIndexInfo = new ArrayList<>(2);
        this.foreignIndexInfo = new ArrayList<>(1);
        this.normalIndexInfo = new ArrayList<>(2);
    }

    public void formatFieldName(String left, String right) {
        if (null == left || null == right) {
            return;
        }
        if (null != this.recordFields) {
            this.recordFields.forEach(recordField -> {
                if (!StringUtils.startsWith(recordField.getFieldName(), left)) {
                    ((DefaultRecordField) recordField).setFieldName(left + recordField.getFieldName() + right);
                }
            });
        }

        this.nameIndex.clear();
        this.recordFields.forEach(recordField -> this.nameIndex.put(recordField.getFieldName(), recordField));
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    @Override
    public List<RecordField> getFields() {
        return recordFields;
    }

    @Override
    public int getFieldCount() {
        return recordFields.size();
    }

    @Override
    public RecordField getField(int index) {
        return recordFields.get(index);
    }

    @Override
    public NullableOptional<RecordField> getField(String fieldName) {

        if (nameIndex == null) {
            return NullableOptional.empty();
        }

        RecordField recordField = nameIndex.getOrDefault(fieldName, null);

        if (null != recordField) {
            return NullableOptional.of(recordField);
        }

        return NullableOptional.empty();
    }

    @Override
    public NullableOptional<RecordField> getFieldIgnoreCase(String fieldName) {

        RecordField recordField = this.ignoreCaseNameIndex.get(fieldName);
        if (null == recordField && (!this.ignoreCaseNameIndex.containsKey(fieldName))) {
            for (Map.Entry<String, RecordField> nameIndex : this.nameIndex.entrySet()) {
                if (fieldName.equalsIgnoreCase(nameIndex.getKey())) {
                    recordField = nameIndex.getValue();
                    break;
                }
            }
            synchronized (this.ignoreCaseNameIndex) {
                if (!this.ignoreCaseNameIndex.containsKey(fieldName)) {
                    this.ignoreCaseNameIndex.put(fieldName, recordField);
                }
            }
        }
        if (null == recordField) {
            return NullableOptional.empty();
        } else {
            return NullableOptional.of(recordField);
        }
    }

    protected List<RecordIndexInfo> removeFiledFromIndexInfoList(List<RecordIndexInfo> indexInfoList, RecordField field) {
        List<RecordIndexInfo> rs = new ArrayList<>();

        for (RecordIndexInfo indexInfo : indexInfoList) {
            if (null == indexInfo) {
                continue;
            }

            indexInfo.removeField(field);

            if (indexInfo.getIndexFields().isEmpty()) {
                continue;
            }

            rs.add(indexInfo);
        }

        return rs;
    }

    @Override
    public void ignoreField(RecordField field) {
        String fieldName = field.getFieldName();
        LOGGER.info("The field {} is ignored.", fieldName);
        if (null != nameIndex.remove(fieldName)) {
            recordFields.remove(field);
        }

        // check primary key
        if (field.isPrimary()) {
            List<RecordIndexInfo> newIndexInfo = removeFiledFromIndexInfoList(Arrays.asList(primaryIndexInfo), field);
            if (null != newIndexInfo && !newIndexInfo.isEmpty()) {
                primaryIndexInfo = newIndexInfo.get(0);
            } else {
                primaryIndexInfo = null;
            }
        }

        // check unique key
        if (field.isUnique()) {
            uniqueIndexInfo = removeFiledFromIndexInfoList(uniqueIndexInfo, field);
        }

        // check normal index
        if (field.isIndexed()) {
            normalIndexInfo = removeFiledFromIndexInfoList(normalIndexInfo, field);
        }
        if (null == this.ignoreRecordFields) {
            this.ignoreRecordFields = new TreeMap<>();
        }
        this.ignoreRecordFields.put(fieldName, field);
    }

    @Override
    public RecordField getIgnoreField(String fieldName) {
        return (null == this.ignoreRecordFields) ? null : this.ignoreRecordFields.get(fieldName);
    }

    @Override
    public List<RawDataType> getRawDataTypes() {
        return recordFields.stream()
            .map(RecordField::getRawDataType)
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getFieldNames() {
        return recordFields.stream()
            .map(RecordField::getFieldName)
            .collect(Collectors.toList());
    }

    @Override
    public NullableOptional<RawDataType> getRawDataType(String fieldName) {
        return getField(fieldName)
            .map(RecordField::getRawDataType);
    }

    @Override
    public NullableOptional<String> getDatabaseName() {
        if (StringUtils.isEmpty(databaseName)) {
            return NullableOptional.empty();
        }

        return NullableOptional.of(databaseName);
    }

    @Override
    public NullableOptional<String> getTableName() {
        if (StringUtils.isEmpty(tableName)) {
            return NullableOptional.empty();
        }

        return NullableOptional.of(tableName);
    }

    public void setFullQualifiedName(String fullQualifiedName) {
        this.fullQualifiedName = fullQualifiedName;
    }

    @Override
    public NullableOptional<String> getFullQualifiedName() {
        if (StringUtils.isEmpty(fullQualifiedName)) {
            if (!StringUtils.isEmpty(databaseName) && !StringUtils.isEmpty(tableName)) {
                return NullableOptional.of(databaseName + "." + tableName);
            }

            return NullableOptional.empty();
        }

        return NullableOptional.of(fullQualifiedName);
    }

    @Override
    public String getSchemaIdentifier() {
        return schemaId;
    }

    public void setPrimaryIndexInfo(RecordIndexInfo indexInfo) {
        this.primaryIndexInfo = indexInfo;
    }

    @Override
    public void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        this.uniqueIndexInfo.add(indexInfo);
    }

    public void addNormalIndexInfo(RecordIndexInfo indexInfo) {
        this.normalIndexInfo.add(indexInfo);
    }

    @Override
    public void addForeignIndexInfo(ForeignKeyIndexInfo indexInfo) {
        this.foreignIndexInfo.add(indexInfo);
    }

    @Override
    public RecordIndexInfo getPrimaryIndexInfo() {
        return primaryIndexInfo;
    }

    @Override
    public List<RecordIndexInfo> getUniqueIndexInfo() {
        return uniqueIndexInfo;
    }

    @Override
    public List<RecordIndexInfo> getNormalIndexInfo() {
        return normalIndexInfo;
    }

    @Override
    public List<ForeignKeyIndexInfo> getForeignIndexInfo() {
        return foreignIndexInfo;
    }

    @Override
    public NullableOptional<String> getSchemaName() {
        return getDatabaseName();
    }

    @Override
    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    @Override
    public void rearrangeRecordFields() {
        RecordField[] fieldArray = new RecordField[recordFields.size()];
        for (RecordField recordField : recordFields) {
            fieldArray[recordField.getFieldPosition()] = recordField;
        }

        recordFields.clear();
        for (int i = 0; i < fieldArray.length; i++) {
            recordFields.add(fieldArray[i]);
        }
    }

    @Override
    public String getFilterCondition() {
        return this.condition;
    }

    public void setFilterCondition(String condition) {
        this.condition = condition;
    }

    @Override
    public List<RecordField> getPartitionFields() {
        return partitionFields;
    }

    public void setPartitionFields(List<RecordField> partitionFields) {
        this.partitionFields = partitionFields;
    }

    @Override
    public String getCharset() {
        return this.charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public String toString() {
        return databaseName + "." + tableName;
    }

    @Override
    public String toPrettyFullString() {
        PrettyOutput output = new PrettyOutput();
        output.begin()
            .addContent("RecordSchema ").addContent(schemaId).addContent(" :").addLine()
            .increaseIdent()
            .addContent("schemaId : ").addContent(schemaId).addLine()
            .addContent("databaseName : ").addContent(databaseName).addLine()
            .addContent("tableName : ").addContent(tableName).addLine()
            .addContent("fullQualifiedName : ").addContent(fullQualifiedName).addLine()
            .addContent("charset : ").addContent(charset).addLine()
            .addContent("totalRows : ").addContent(totalRows).addLine()
            .addContent("fields :").addLine()
                .increaseIdent()
                .addCollectionItemPerLine(recordFields)
                .decreaseIdent();
        if (null != primaryIndexInfo) {
            output.addContent("primaryIndexInfo : ").addContent(primaryIndexInfo).addLine();
        }
        if (null != uniqueIndexInfo && !uniqueIndexInfo.isEmpty()) {
            output.addContent("uniqueIndexInfo : ").addLine()
                .increaseIdent()
                .addCollectionItemPerLine(uniqueIndexInfo)
                .decreaseIdent();
        }
        if (null != foreignIndexInfo && !foreignIndexInfo.isEmpty()) {
            output.addContent("foreignIndexInfo : ").addLine()
                .increaseIdent()
                .addCollectionItemPerLine(foreignIndexInfo)
                .decreaseIdent();
        }
        if (null != partitionFields && !partitionFields.isEmpty()) {
            output.addContent("partitionFields : ").addContent(partitionFields, RecordField::getFieldName).addLine();
        }
        if (null != condition) {
            output.addContent("condition : ").addContent(condition);
        }
        return output.getOutput();
    }
}
