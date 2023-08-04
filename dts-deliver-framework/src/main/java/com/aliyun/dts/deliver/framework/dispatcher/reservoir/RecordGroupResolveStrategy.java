package com.aliyun.dts.deliver.framework.dispatcher.reservoir;

import com.aliyun.dts.deliver.DtsContext;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.framework.dispatcher.ToExecuteRecordBatchQueue;
import org.apache.commons.lang3.tuple.Pair;

public interface RecordGroupResolveStrategy {

    Pair<String, Long> getReplicatedCheckpoint();

    public abstract void initialize(Settings settings, ToExecuteRecordBatchQueue resolvedTransactions,
                                    boolean maintainCheckpoint, boolean maintainTicket, DtsContext context);

    /**
     * Resolve traceableRecord, such as ddl dml .
     */
    void resolve(DtsMessage dtsMessage);
}
