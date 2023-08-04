package com.aliyun.dts.deliver.protocol.record.util;

import com.aliyun.dts.deliver.protocol.record.*;
import com.aliyun.dts.deliver.protocol.record.value.Value;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RecordTools {
    private static final long TELL_MILLISECOND_VALUE = 9999999999L;

    public static String toMillis(long timestamp) {
        if (timestamp > TELL_MILLISECOND_VALUE) {
            return String.valueOf(timestamp);
        }
        return Long.toString(TimeUnit.SECONDS.toMillis(timestamp));
    }

    public static boolean isKeyChange(Record record) {
        if (record.getOperationType() == OperationType.UPDATE) {
            RecordSchema recordSchema = record.getSchema();
            RecordIndexInfo primaryIndexInfo = recordSchema.getPrimaryIndexInfo();
            if (isKeyFieldsChanged(primaryIndexInfo, record.getBeforeImage(), record.getAfterImage())) {
                return true;
            }
            List<RecordIndexInfo> ukRecordIndexInfos = recordSchema.getUniqueIndexInfo();
            for (RecordIndexInfo ukRecordIndexInfo : ukRecordIndexInfos) {
                if (isKeyFieldsChanged(ukRecordIndexInfo, record.getBeforeImage(), record.getAfterImage())) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    public static boolean isKeyFieldsChanged(RecordIndexInfo recordIndexInfo, RowImage beforeImage, RowImage afterImage) {
        // record without pk should not be merged
        if (null == recordIndexInfo) {
            return true;
        }
        for (RecordField recordField : recordIndexInfo.getIndexFields()) {
            Value beforeValue = beforeImage.getValue(recordField.getFieldName()).get();
            Value afterValue = afterImage.getValue(recordField.getFieldName()).get();
            if (!valueEquals(beforeValue, afterValue)) {
                return true;
            }
        }
        return false;
    }


    public static boolean valueEquals(Value src, Value dest) {
        if (src == null && dest == null) {
            return true;
        }
        if ((src == null && dest != null) || (src != null && dest == null)) {
            return false;
        }
        return src.toString().equals(dest.toString());
    }

    public static String getDBName(Record record) {
        return record.getSchema().getDatabaseName().orElse(null);
    }

    public static String getSchemaName(Record record) {
        return record.getSchema().getSchemaName().orElse(null);
    }

    public static String getTableName(Record record) {
        return record.getSchema().getTableName().orElse(null);
    }

    /**
     * Get ddl string contained in current record.
     */
    public static String getDDLString(Record record) {
        Preconditions.checkArgument(record.getOperationType() == OperationType.DDL);
        return record.getAfterImage().getValues()[0].toString();
    }

}
