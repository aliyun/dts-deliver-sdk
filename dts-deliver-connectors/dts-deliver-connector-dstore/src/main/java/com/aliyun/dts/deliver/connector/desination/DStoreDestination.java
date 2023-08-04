package com.aliyun.dts.deliver.connector.desination;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.DtsMessageConsumer;
import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.openapi.DtsOpenApi;
import com.aliyun.dts.deliver.connector.BaseConnector;
import com.aliyun.dts.deliver.protocol.generated.DtsConnectionStatus;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DStoreDestination extends BaseConnector implements Destination {
    private static final Logger LOG = LoggerFactory.getLogger(DStoreDestination.class);

    private Settings settings;
    private Pair<String, String> userPassword;

    private DtsMessageConsumer dtsMessageConsumer;

    @Override
    public void open(Settings settings, long sinkerId) throws Exception {
        this.settings = settings;

        DtsOpenApi dtsOpenApi = new DtsOpenApi(
                GlobalSettings.ALIYUN_AK.getValue(settings),
                GlobalSettings.ALIYUN_SECRET.getValue(settings),
                GlobalSettings.DTS_JOB_ID.getValue(settings),
                GlobalSettings.DTS_OPENAPI_REGION.getValue(settings)
        );

        String userName = GlobalSettings.DTS_DELIVER_USER.getValue(settings);
        String password = GlobalSettings.DTS_DELIVER_PASSWORD.getValue(settings);

        if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)){
            LOG.info("user and password is not null, just use it");
            dtsOpenApi.setUserName(userName);
            dtsOpenApi.setPassword(password);
        }

        this.userPassword = dtsOpenApi.getUserPassword();

        dtsMessageConsumer = getConsumer(sinkerId);
    }

    @Override
    //todo(yanmen)
    public DtsConnectionStatus check(JsonNode config) throws Exception {
        return new DtsConnectionStatus().withStatus(DtsConnectionStatus.Status.SUCCEEDED);
    }

    @Override
    public void close() {
    }

    @Override
    public DtsMessageConsumer getConsumer(long sinkId) throws Exception {

        return new DStoreRecordConsumer(settings, DStoreDestinationConfig.getDStoreDestinationConfig(settings, userPassword, sinkId));
    }

    @Override
    public Boolean isRecoverable(Throwable e) {
        return false;
    }

    @Override
    public Future<Void> accept(final List<Record> msg) throws Exception {
        return dtsMessageConsumer.accept(msg);
    }

}
