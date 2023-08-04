package com.aliyun.dts.deliver.core.runtime.pipeline;
import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;

import java.util.List;

public class DtsMessagePipeline {

    // use to build interceptor class
    private List<DtsMessageInterceptor> recordInterceptors;

    public DtsMessagePipeline(List<DtsMessageInterceptor> recordInterceptors) {
        this.recordInterceptors = recordInterceptors;
    }

    private DtsMessage processDtsMessage(DtsMessage dtsMessage) {
        for (DtsMessageInterceptor recordInterceptor : recordInterceptors) {
            DtsMessage interceptedDtsMessage = recordInterceptor.intercept(dtsMessage);
            if (null == dtsMessage) {
                break;
            }

            dtsMessage = interceptedDtsMessage;
        }

        return dtsMessage;
    }

    private List<DtsMessage> processDtsMessages(List<DtsMessage> dtsMessages) {
        List<DtsMessage> outputDtsMessages = dtsMessages;

        for (DtsMessageInterceptor recordInterceptor : recordInterceptors) {
            outputDtsMessages = recordInterceptor.intercept(dtsMessages);
            if (null == outputDtsMessages) {
                break;
            }

            dtsMessages = outputDtsMessages;
        }
        return outputDtsMessages;
    }

    protected static DtsMessage realCookDtsMessage(DtsMessage dtsMessage, DtsMessagePipeline recordPipeline) {
        if (null != recordPipeline) {
            return recordPipeline.processDtsMessage(dtsMessage);
        }

        return dtsMessage;
    }

    public static DtsMessage cookDtsMessage(DtsMessage dtsMessage, DtsMessagePipeline recordPipeline) {
        return realCookDtsMessage(dtsMessage, recordPipeline);
    }

    private static List<DtsMessage> realCookDtsMessages(List<DtsMessage> dtsMessages, DtsMessagePipeline pipeline) {
        if (null != pipeline) {
            return pipeline.processDtsMessages(dtsMessages);
        }

        return dtsMessages;
    }

    public static List<DtsMessage> cookDtsMessages(List<DtsMessage> dtsMessages, DtsMessagePipeline pipeline) {
        return realCookDtsMessages(dtsMessages, pipeline);
    }
}
