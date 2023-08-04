package com.aliyun.dts.deliver.protocol.record.value;

public enum ObjectType {

    BINARY, BOOL, BLOB, XML, JSON, JSONB, TEXT, BFILE, RAW, LONG_RAW, ROWID, UROWID, ENUM, SET, BYTEA, ORIGINAL_BYTEA, GEOMETRY, XTYPE, DOCUMENT;

    public static ObjectType parse(String type) {

        if (null == type) {
            return XTYPE;
        }
        type = type.toUpperCase();

        ObjectType[] objectTypes = ObjectType.values();
        for (ObjectType objectType : objectTypes) {
            if (objectType.name().equals(type)) {
                return objectType;
            }
        }
        return XTYPE;
    }
}
