package com.aliyun.dts.deliver.test;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.DtsMessageConsumer;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.framework.processor.FutureRecords;
import com.aliyun.dts.deliver.framework.processor.InflightRecord;
import com.aliyun.dts.deliver.protocol.generated.ConnectorSpecification;
import com.aliyun.dts.deliver.protocol.generated.DtsConnectionStatus;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class IaDummyDestition implements Destination {
    private static final Logger LOG = LogManager.getLogger(IaDummyDestition.class);

    public IaDummyDestition() {
    }

    @Override
    public DtsMessageConsumer getConsumer(long l) throws Exception {
        return null;
    }

    @Override
    public Boolean isRecoverable(Throwable throwable) {
        return null;
    }

    @Override
    public void open(Settings settings, long l) throws Exception {

    }

    @Override
    public Future<Void> accept(List<Record> list) throws Exception {
        FutureRecords<Void> futureRecords = new FutureRecords<>(list);
        for (final InflightRecord inflightRecord : futureRecords.getInFlightRequests()) {
            LOG.info("write: {}", inflightRecord.toString());
            futureRecords.complete(inflightRecord);
        }
        return futureRecords;
    }

    @Override
    public ConnectorSpecification spec() throws Exception {
        return null;
    }

    @Override
    public DtsConnectionStatus check(JsonNode jsonNode) throws Exception {
        return null;
    }

    @Override
    public void close() {

    }
}
