package com.aliyun.dts.deliver;

import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.feature.Configurable;
import com.aliyun.dts.deliver.protocol.record.Record;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.metrics.Metrics;

import java.util.ArrayList;
import java.util.List;

public interface RecordInterceptor {

    String name();

    Record intercept(Record record);

    default Pair<List<Record>, Integer> intercept(List<Record> records) {
        List<Record> outputRecords = new ArrayList<>(records.size());

        for (Record record : records) {
            Record newRecord = intercept(record);
            if (null != newRecord) {
                outputRecords.add(newRecord);
            }
        }
        return Pair.of(outputRecords, outputRecords.size());
    }

    default Pair<List<Record>, Integer> intercept(List<Record> records, Configurable configurable) {
        return intercept(records);
    }

    void initialize(Settings settings, Metrics metrics);
}
