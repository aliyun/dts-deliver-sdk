package com.aliyun.dts.deliver.test;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.config.GlobalSettings;
import com.aliyun.dts.deliver.commons.config.JobConfig;
import com.aliyun.dts.deliver.connector.desination.DStoreDestination;
import com.aliyun.dts.deliver.core.bootstrap.DtsDeliver;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.resolver.internal.RecordGroupSender;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DtsDeliverTest {
    private static final Logger LOG = LoggerFactory.getLogger(DtsDeliverTest.class);

    public static void main(String[] args) throws Throwable  {

        String configPath = "";
        Map<String, String> settingValueMap = new HashMap<>();

        settingValueMap.put(GlobalSettings.DTS_BOOTSTRAP_SERVERS_CONFIG.getKey(), "47.110.194.196:18001");
        settingValueMap.put(GlobalSettings.ALIYUN_AK.getKey(), "LTAI5t6DjefZ8RXwqyDWPWqK");
        settingValueMap.put(GlobalSettings.ALIYUN_SECRET.getKey(), "9taQu4gH4Mo2alL8JUuaHcM7LrdxLQ");

        settingValueMap.put(GlobalSettings.DTS_DELIVER_USER.getKey(), "u7rbg46dnsxl-dtsx4pi76su25896x1");
        settingValueMap.put(GlobalSettings.DTS_DELIVER_PASSWORD.getKey(), "MTIRhpF4SQD0R5ZRYCm/5A==");

        settingValueMap.put(GlobalSettings.DTS_JOB_ID.getKey(), "bbdx7yqb25dl11s");
        settingValueMap.put(GlobalSettings.DTS_DELIVER_TOPIC.getKey(), "cn_hangzhou_vpc_bbdx7yqb25dl11s_data_delivery_version2");
        settingValueMap.put(GlobalSettings.DTS_DELIVER_TOPIC_PARTITION_NUM.getKey(), "3");
        settingValueMap.put(GlobalSettings.DTS_OPENAPI_REGION.getKey(), "cn-hangzhou");

        JobConfig jobConfig = new JobConfig(configPath, settingValueMap);

        //source
        List<Source> sourceList = new ArrayList<>();
        Source source1 = new FakeSource("source 1", jobConfig.getSettings(), "dts_deliver_test", "tab1", 100);
        Source source2 = new FakeSource("source 2", jobConfig.getSettings(), "dts_deliver_test", "tab2", 200);
        Source source3 = new FakeSource("source 3", jobConfig.getSettings(), "dts_deliver_test", "tab3", 300);
        sourceList.add(source1);
        sourceList.add(source2);
        sourceList.add(source3);

        Destination destination = new DStoreDestination();

        Consumer<List<Pair<String, RecordCheckpoint>>> checkpointConsumer = (checkpoints) ->{
            saveCheckPoints(checkpoints);
        };

        DtsDeliver dtsDeliver = new DtsDeliver(jobConfig, sourceList, destination, checkpointConsumer);

        dtsDeliver.startup();

        LOG.info("sleep...");
        Thread.sleep(10 * 1000);

        dtsDeliver.stop();

        LOG.info("stoped");
    }

    //todo, such as file or database to save the checkpoint
    private static void saveCheckPoints(List<Pair<String, RecordCheckpoint>> checkpoints) {
        LOG.info("checkpoints: " + checkpoints);
    }
}
