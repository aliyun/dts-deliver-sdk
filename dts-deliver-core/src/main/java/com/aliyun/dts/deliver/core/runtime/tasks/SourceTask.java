package com.aliyun.dts.deliver.core.runtime.tasks;

import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.exceptions.DtsCoreException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.commons.util.AutoCloseableIterator;
import com.aliyun.dts.deliver.core.runtime.pipeline.DtsMessagePipeline;
import com.aliyun.dts.deliver.protocol.generated.ConfiguredDtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.store.AbstractRecordStoreWithMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SourceTask extends Task {
    public static final String BASE_TASK_NAME = "source";
    private static final AtomicLong SOURCE_TASK_ID_GENERATOR = new AtomicLong(0);

    private static final Logger LOG = LoggerFactory.getLogger(SourceTask.class);

    private static RateLimiter rpsThrottle = null;
    private static RateLimiter bpsThrottle = null;

    private final Source source;

    private final AbstractRecordStoreWithMetrics recordStore;

    private final DtsMessagePipeline sourcePipeline;
    private final Settings settings;
    public SourceTask(Settings settings, Source source, AbstractRecordStoreWithMetrics recordStore,
                      DtsMessagePipeline sourcePipeline) {
        super(BASE_TASK_NAME, SOURCE_TASK_ID_GENERATOR.incrementAndGet());

        this.settings = settings;
        this.source = source;
        this.recordStore = recordStore;
        this.sourcePipeline = sourcePipeline;
    }

    public static synchronized void setRpsThrottle(double maxQps) {
        if (maxQps > 0) {
            if (rpsThrottle == null) {
                rpsThrottle = RateLimiter.create(maxQps);
            } else {
                rpsThrottle.setRate(maxQps);
            }
        } else {
            rpsThrottle = null;
        }
    }

    public static void setBpsThrottle(double maxBps) {
        if (maxBps > 0) {
            if (bpsThrottle == null) {
                bpsThrottle = RateLimiter.create(maxBps);
            } else {
                bpsThrottle.setRate(maxBps);
            }
        } else {
            bpsThrottle = null;
        }
    }

    private void realRunRoutine() throws Exception {
        try {
            source.open();

            while (!isStopped()) {

                inState("Read");

                //todo(yanmen)
                ConfiguredDtsCatalog catalog = null;
                JsonNode state = null;
                AutoCloseableIterator<DtsMessage> messageIterator = source.read(settings, catalog, state);

                if (null == messageIterator || !messageIterator.hasNext()) {
                    continue;
                }

                inState("Write");

                messageIterator.forEachRemaining(dtsMessage -> {
                    DtsMessage cookDtsMessage = DtsMessagePipeline.cookDtsMessage(dtsMessage, sourcePipeline);
                    recordStore.publish(cookDtsMessage);
                });
            }
        } catch (Throwable e) {
            // something wrong, we should close any source before reusing it
            source.close();
            throw e;
        }
    }

    @Override
    public void safeRun() {

        try {
            context.retry(() -> realRunRoutine(),
                    (e, t) -> !isStopped() && source.isRecoverable(e));
        } catch (Throwable e) {
            LOG.error("process failed according to {}", getTaskName(), e.getMessage());
            throw new DtsCoreException(ErrorCode.FRAMEWORK_READ_SOURCE_DATA, "In process of processing data failed", e);
        } finally {
            SwallowException.callAndSwallowException(() -> source.close());
            LOG.info("{} release", getTaskName());
        }
    }

    void rpsThrottle(long readRows) {
        if (rpsThrottle == null || readRows <= 0) {
            return;
        }

        inState("Throttle");

        double waitTime = 0;
        while (readRows > Integer.MAX_VALUE) {
            waitTime += rpsThrottle.acquire(Integer.MAX_VALUE);
            readRows -= Integer.MAX_VALUE;
        }
        waitTime += rpsThrottle.acquire((int) readRows);
        LOG.debug("The RPS throttle involved for {} sec.", waitTime);
    }

    void bpsThrottle(long totalSize) {
        if (bpsThrottle == null || totalSize < 1) {
            return;
        }

        inState("Throttle");

        double waitTime = 0;
        while (totalSize > Integer.MAX_VALUE) {
            waitTime += bpsThrottle.acquire(Integer.MAX_VALUE);
            totalSize -= Integer.MAX_VALUE;
        }
        waitTime += bpsThrottle.acquire((int) totalSize);
        LOG.debug("The BPS throttle involved for {} sec.", waitTime);
    }
}
