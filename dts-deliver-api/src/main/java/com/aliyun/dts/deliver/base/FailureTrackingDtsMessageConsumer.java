package com.aliyun.dts.deliver.base;


import com.aliyun.dts.deliver.commons.concurrency.Future;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Minimal abstract class intended to provide a consistent structure to classes seeking to implement
 * the {@link DtsMessageConsumer} interface. The original interface methods are wrapped in
 * generic exception handlers - any exception is caught and logged.
 *
 * Two methods are intended for extension:
 * <ul>
 * <li>startTracked: Wraps set up of necessary infrastructure/configuration before message
 * consumption.</li>
 * <li>acceptTracked: Wraps actual processing of each
 * </ul>
 *
 * Though not necessary, we highly encourage using this class when implementing destinations. See
 * child classes for examples.
 */
public abstract class FailureTrackingDtsMessageConsumer implements DtsMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureTrackingDtsMessageConsumer.class);

    private boolean hasFailed = false;

    /**
     * Wraps setup of necessary infrastructure/configuration before message consumption
     *
     * @throws Exception
     */
    protected abstract void startTracked() throws Exception;

    @Override
    public void start() throws Exception {
        try {
            startTracked();
        } catch (final Exception e) {
            LOGGER.error("Exception while starting consumer", e);
            hasFailed = true;
            throw e;
        }
    }

    /**
     * Processing of DtsMessages with general functionality of storing STATE messages, serializing
     * RECORD messages and storage within a buffer
     *
     * NOTE: Not all the functionality mentioned above is always true but generally applies
     *
     * @param msg {@link DtsMessage} to be processed
     * @throws Exception
     */
    protected abstract Future<Void> acceptTracked(List<Record> msg) throws Exception;

    @Override
    public Future<Void> accept(final List<Record> msg) throws Exception {
        try {
            return acceptTracked(msg);
        } catch (final Exception e) {
            LOGGER.error("Exception while accepting message", e);
            hasFailed = true;
            throw e;
        }
    }

    protected abstract void close(boolean hasFailed) throws Exception;

    @Override
    public void close() throws Exception {
        if (hasFailed) {
            LOGGER.warn("Dts message consumer: failed.");
        } else {
            LOGGER.info("Dts message consumer: succeeded.");
        }
        close(hasFailed);
    }

}
