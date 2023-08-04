package com.aliyun.dts.deliver.core.runtime.pipeline;

import com.aliyun.dts.deliver.RecordInterceptor;
import com.aliyun.dts.deliver.protocol.record.Record;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class RecordPipeline {
    public static final String ETL_PIPELINE_CONFIG = "etlPipeConfig";

    // use to build interceptor class
    private List<RecordInterceptor> recordInterceptors;

    public RecordPipeline(List<RecordInterceptor> recordInterceptors) {
        this.recordInterceptors = recordInterceptors;
    }

    private Pair<List<Record>, Integer> processRecords(List<Record> records) {

        List<Record> outputRecords = records;
        int affectedRecords = 0;

        for (RecordInterceptor recordInterceptor : recordInterceptors) {
            Pair<List<Record>, Integer> interceptedPair = recordInterceptor.intercept(records);
            outputRecords = interceptedPair.getLeft();
            affectedRecords = interceptedPair.getRight();
            if (null == outputRecords || outputRecords.isEmpty()) {
                break;
            }

            records = outputRecords;
        }

        return Pair.of(outputRecords, affectedRecords);
    }

    protected static Pair<List<Record>, Integer> realCookRecords(List<Record> records, RecordPipeline recordPipeline) {
        if (null != recordPipeline) {
            return recordPipeline.processRecords(records);
        }

        return Pair.of(records, records.size());
    }

    public static Pair<List<Record>, Integer> cookRecords(List<Record> records, RecordPipeline recordPipeline) {
        return realCookRecords(records, recordPipeline);
    }
}
