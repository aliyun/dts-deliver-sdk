package com.aliyun.dts.deliver.protocol.record.impl.ddl;

import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.RecordSchema;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.protocol.record.impl.*;
import com.aliyun.dts.deliver.protocol.record.value.StringValue;

import java.util.Arrays;

public class DDLRecord {
    public static DefaultRecord buildDDLRecord(String jsonSql, String schemaName, String tableName, RecordCheckpoint recordCheckpoint, RecordHeader recordHeader){

        checkJsonSqlValid(jsonSql);

        //schema
        RecordSchema recordSchema = buildDDLRecordSchema(schemaName, tableName);

        DefaultRecord ddlRecord = new DefaultRecord(OperationType.DDL, recordSchema, recordHeader);

        //after image
        DefaultRowImage afterImage = new DefaultRowImage(recordSchema);

        afterImage.setValue(0, new StringValue(jsonSql));

        ddlRecord.setAfterImage(afterImage);

        ddlRecord.setRecordCheckpoint(recordCheckpoint);

        return ddlRecord;
    }

    //todo(yanmen) check if the json sql is valid
    private static void checkJsonSqlValid(String jsonSql) {
    }

    private static RecordSchema buildDDLRecordSchema(String schemaName, String tableName) {
        DefaultRecordField recordFiled = DefaultRecordField.builder()
                .withFieldName("ddl")
                .withRawDataType(DefaultRawDataType.of("STRING", 1))
                .get();
        RecordSchema recordSchema = new DefaultRecordSchema("1", schemaName, tableName, Arrays.asList(recordFiled));
        return recordSchema;
    }
}
