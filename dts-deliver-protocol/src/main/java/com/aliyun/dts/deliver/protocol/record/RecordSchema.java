package com.aliyun.dts.deliver.protocol.record;

import com.aliyun.dts.deliver.commons.util.NullableOptional;

import java.util.List;

public interface RecordSchema {
    /**
     * get the database info the record schema refers to.
     */
    DatabaseInfo getDatabaseInfo();

    /**
     * @return the list of fields that are present in the schema
     */
    List<RecordField> getFields();

    /**
     * @return the number of fields in the schema
     */
    int getFieldCount();

    /**
     * @param index the 0-based index of which field to return
     * @return the index'th field
     * @throws IndexOutOfBoundsException if the index is < 0 or >= the number of fields (determined by {@link #getFieldCount()})
     */
    RecordField getField(int index);

    /**
     * @param fieldName the name of the field
     * @return an Optional RecordField for the field with the given name
     */
    NullableOptional<RecordField> getField(String fieldName);

    /**
     * @param fieldName the name of the field
     * @return an Optional RecordField for the field with the given name
     */
    default NullableOptional<RecordField> getFieldIgnoreCase(String fieldName) {
        return getField(fieldName);
    }

    /**
     * mark the @filed to be ignored, so the user can not see it any more
     *
     * @param field to be ignored
     */
    void ignoreField(RecordField field);

    /**
     * Get ignore recordField with etl
     * @param fieldName
     * @return
     */
    RecordField getIgnoreField(String fieldName);

    /**
     * @return the raw data types of the fields
     */
    List<RawDataType> getRawDataTypes();

    /**
     * @return the names of the fields
     */
    List<String> getFieldNames();

    /**
     * @return the name of new add fieldNames for etl
     */
    default List<String> getETLNewFieldNames() {
        return null;
    }

    /**
     * @param fieldName the name of the field whose type is desired
     * @return the RecordFieldType associated with the field that has the given name, or <code>null</code> if the schema does not contain a field with the given name
     */
    NullableOptional<RawDataType> getRawDataType(String fieldName);

    /**
     * @return the full name with qualified character of current record schema
     */
    NullableOptional<String> getFullQualifiedName();

    /**
     * @return the table name
     */
    NullableOptional<String> getDatabaseName();

    /**
     * get schema name
     */
    NullableOptional<String> getSchemaName();

    /**
     * getTableName
     */
    NullableOptional<String> getTableName();

    /**
     * @return the id for this schema
     */
    String getSchemaIdentifier();

    /**
     * get the primary key info.
     */
    RecordIndexInfo getPrimaryIndexInfo();

    /**
     * get all foreign key info, may be the foreign key refers to a primary key with multi cols, so we
     * use RecordIndexInfo to represent it.
     */
    List<ForeignKeyIndexInfo> getForeignIndexInfo();

    /**
     * get all unique key info.
     */
    List<RecordIndexInfo> getUniqueIndexInfo();

    /**
     * get all normal indexes(which means it's not pk, uk and fk).
     */
    List<RecordIndexInfo> getNormalIndexInfo();

    /**
     * rearrange the record fields according to record field position.
     */
    void rearrangeRecordFields();

    /**
     * get the estimated total rows in current record schema.
     */
    default long getTotalRows() {
        return 0L;
    }

    /**
     * get filter condition belongs to record schema, which is used to filter records
     */
    String getFilterCondition();

    default List<RecordField> getPartitionFields() {
        return null;
    }

    default void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        throw new RuntimeException("not impl");
    }

    default void addForeignIndexInfo(ForeignKeyIndexInfo indexInfo) {
        throw new RuntimeException("not impl");
    }

    /**
     * get the table charset
     */
    String getCharset();

    default short crc16IdForName() {
        return (short) 0;
    }

    /**
     * short string to summary current object
     */
    String toString();

    /**
     * long pretty string for current object to print
     */
    String toPrettyFullString();
}
