package com.aliyun.dts.deliver.framework.dispatcher.record.batch;


import com.aliyun.dts.deliver.protocol.record.Record;

import java.util.List;

public interface RecordBatch {

    RecordBatchType getRecordBatchType();

    List<Record> getRecordList();
}
