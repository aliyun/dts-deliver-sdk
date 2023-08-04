package com.aliyun.dts.deliver;

import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import org.apache.kafka.common.metrics.Metrics;

import java.util.List;

public interface DtsMessageInterceptor {

    String name();

    void initialize(Settings settings);

    DtsMessage intercept(DtsMessage record);

    List<DtsMessage> intercept(List<DtsMessage> dtsMessages);
}
