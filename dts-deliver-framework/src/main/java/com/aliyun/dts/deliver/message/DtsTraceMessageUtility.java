package com.aliyun.dts.deliver.message;

import com.aliyun.dts.deliver.base.Destination;
import com.aliyun.dts.deliver.protocol.generated.DtsErrorTraceMessage;
import com.aliyun.dts.deliver.protocol.generated.DtsErrorTraceMessage.FailureType;
import com.aliyun.dts.deliver.protocol.generated.DtsEstimateTraceMessage;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage.Type;
import com.aliyun.dts.deliver.protocol.generated.DtsTraceMessage;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.function.Consumer;

public class DtsTraceMessageUtility {

    private DtsTraceMessageUtility() {}

    public static void emitSystemErrorTrace(final Throwable e, final String displayMessage) {
        emitErrorTrace(e, displayMessage, FailureType.SYSTEM_ERROR);
    }

    public static void emitConfigErrorTrace(final Throwable e, final String displayMessage) {
        emitErrorTrace(e, displayMessage, FailureType.CONFIG_ERROR);
    }

    public static void emitEstimateTrace(final int byteEstimate,
                                         final DtsEstimateTraceMessage.Type type,
                                         final int rowEstimate,
                                         final String streamName,
                                         final String streamNamespace) {
        emitMessage(makeDtsMessageFromTraceMessage(
                makeDtsTraceMessage(DtsTraceMessage.Type.ESTIMATE)
                        .withEstimate(new DtsEstimateTraceMessage()
                                .withByteEstimate(byteEstimate)
                                .withType(type)
                                .withRowEstimate(rowEstimate)
                                .withName(streamName)
                                .withNamespace(streamNamespace))));
    }

    public static void emitErrorTrace(final Throwable e, final String displayMessage, final FailureType failureType) {
        emitMessage(makeErrorTraceDtsMessage(e, displayMessage, failureType));
    }

    // todo: handle the other types of trace message we'll expect in the future, see
    // io.dts.protocol.models.v0.DtsTraceMessage
    // & the tech spec:
    // https://docs.google.com/document/d/1ctrj3Yh_GjtQ93aND-WH3ocqGxsmxyC3jfiarrF6NY0/edit#
    // public void emitNotificationTrace() {}
    // public void emitMetricTrace() {}

    private static void emitMessage(final DtsMessage message) {
        // Not sure why defaultOutputRecordCollector is under Destination specifically,
        // but this matches usage elsewhere in base-java
        final Consumer<DtsMessage> outputRecordCollector = Destination::defaultOutputRecordCollector;
        outputRecordCollector.accept(message);
    }

    private static DtsMessage makeErrorTraceDtsMessage(
            final Throwable e,
            final String displayMessage,
            final FailureType failureType) {

        return makeDtsMessageFromTraceMessage(
                makeDtsTraceMessage(DtsTraceMessage.Type.ERROR)
                        .withError(new DtsErrorTraceMessage()
                                .withFailureType(failureType)
                                .withMessage(displayMessage)
                                .withInternalMessage(e.toString())
                                .withStackTrace(ExceptionUtils.getStackTrace(e))));
    }

    private static DtsMessage makeDtsMessageFromTraceMessage(final DtsTraceMessage dtsTraceMessage) {
        return new DtsMessage().withType(Type.TRACE).withTrace(dtsTraceMessage);
    }

    private static DtsTraceMessage makeDtsTraceMessage(final DtsTraceMessage.Type traceMessageType) {
        return new DtsTraceMessage().withType(traceMessageType).withEmittedAt((double) System.currentTimeMillis());
    }

}
