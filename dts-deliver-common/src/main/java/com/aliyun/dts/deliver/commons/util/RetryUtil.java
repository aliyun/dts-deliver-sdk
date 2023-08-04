package com.aliyun.dts.deliver.commons.util;

import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.commons.exceptions.ExceptionUtil;
import com.aliyun.dts.deliver.commons.exceptions.FatalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.RecoverableDtsException;
import com.aliyun.dts.deliver.commons.functional.SwallowException;
import com.aliyun.dts.deliver.commons.functional.ThrowableFunction;
import com.aliyun.dts.deliver.commons.functional.ThrowableFunctionVoid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class RetryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);
    private final String globalJobType;
    private final String objectNameShouldBeRetried;
    private final long maxRetrySeconds;
    private Function<Throwable, Boolean> recoverableChecker;
    private int maxRetryTimes;
    private int freezeInterval;
    private TimeUnit freezeTimeUnit;
    private int blindRetryTimes;

    public RetryUtil(Function<Throwable, Boolean> recoverableChecker) {
        this(5, TimeUnit.SECONDS, 1, recoverableChecker);
    }

    public RetryUtil(int freezeInterval, TimeUnit freezeTimeUnit, int maxRetryTimes,
                     Function<Throwable, Boolean> recoverableChecker) {
        this("unknown", "unknown", freezeInterval, freezeTimeUnit, maxRetryTimes, Long.MAX_VALUE, recoverableChecker);
    }

    public RetryUtil(String globalJobType, String objectNameShouldBeRetried,
                     int freezeInterval, TimeUnit freezeTimeUnit, int maxRetryTimes,
                     long maxRetrySeconds,
                     Function<Throwable, Boolean> recoverableChecker) {
        this(globalJobType, objectNameShouldBeRetried, freezeInterval, freezeTimeUnit, 3, maxRetryTimes, maxRetrySeconds, recoverableChecker);
    }

    public RetryUtil(String globalJobType, String objectNameShouldBeRetried,
                     int freezeInterval, TimeUnit freezeTimeUnit, int blindRetryTimes, int maxRetryTimes,
                     long maxRetrySeconds,
                     Function<Throwable, Boolean> recoverableChecker) {
        this.globalJobType = globalJobType;
        this.objectNameShouldBeRetried = objectNameShouldBeRetried;
        this.maxRetrySeconds = maxRetrySeconds;
        this.blindRetryTimes = Math.min(blindRetryTimes, maxRetryTimes);
        this.maxRetryTimes = maxRetryTimes;
        this.freezeInterval = Math.max(freezeInterval, 1);
        this.freezeTimeUnit = freezeTimeUnit;
        this.recoverableChecker = recoverableChecker;
    }

    public void callFunctionWithRetry(ThrowableFunctionVoid throwableFunction) throws Exception {
        callFunctionWithRetry(throwableFunction, (e, times) -> recoverableChecker.apply(e), null);
    }

    public void callFunctionWithRetry(ThrowableFunctionVoid throwableFunction,
                                      BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                      BiConsumer<RetryInfo, Long> retryInfoConsumerWithExpireTime) throws Exception {
        callFunctionWithRetry(
            () -> {
                throwableFunction.call();
                return null;
            },
            recoverableChecker, retryInfoConsumerWithExpireTime);
    }

    public <T> T callFunctionWithRetry(ThrowableFunction<T> throwableFunction) throws Exception {
        return callFunctionWithRetry(maxRetryTimes, freezeInterval, freezeTimeUnit, throwableFunction);
    }

    public <T> T callFunctionWithRetry(int maxRetryTimes, int freezeInternal, TimeUnit freezeTimeUnit,
                                       ThrowableFunction<T> throwableFunction) throws Exception {
        return this.callFunctionWithRetry(0, maxRetryTimes, freezeInternal, freezeTimeUnit, throwableFunction,
            (e, times) -> recoverableChecker.apply(e), null);
    }

    public <T> T callFunctionWithRetry(ThrowableFunction<T> throwableFunction,
                                       BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                       BiConsumer<RetryInfo, Long> retryInfoConsumerWithExpireTime) throws Exception {
        return callFunctionWithRetry(blindRetryTimes, maxRetryTimes, freezeInterval, freezeTimeUnit, throwableFunction, recoverableChecker, retryInfoConsumerWithExpireTime);
    }

    public <T> T callFunctionWithRetry(int blindRetryTimes, int maxRetryTimes, int freezeInternal, TimeUnit freezeTimeUnit,
                                       ThrowableFunction<T> throwableFunction,
                                       BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                       BiConsumer<RetryInfo, Long> retryInfoConsumerWithExpireTime) throws Exception {
        boolean isFatalError = false;
        Throwable error = null;
        RetryInfo retryInfo = null;
        maxRetryTimes = Math.max(1, maxRetryTimes);
        int retryTime = 0;
        TimeUtils.TimeExpireJudge timeExpireJudge = TimeUtils.getTimeExpireJudge(Time.getInstance(), maxRetrySeconds, TimeUnit.SECONDS);

        timeExpireJudge.start();
        for (retryTime = 0; retryTime < maxRetryTimes; retryTime++) {
            try {
                T rs = throwableFunction.call();
                if (null != retryInfo) {
                    retryInfo.endRetry();
                }
                return rs;
            } catch (Throwable e) {
                error = e;

                if (ExceptionUtil.INSTANCE.isFatalException(e)) {
                    LOG.warn("call function {} with {} times failed, stop retry according to fatal error",
                        throwableFunction,
                        null == retryInfo ? 0 : retryInfo.getRetryCount(),
                        e);
                    isFatalError = true;
                    break;
                } else {
                    // do retry check logic
                    boolean blindChecked = false;
                    boolean shouldRetry = recoverableChecker != null && recoverableChecker.apply(e, retryTime);
                    if (!shouldRetry) {
                        shouldRetry = determineCanBlindRetry(retryTime, blindRetryTimes, recoverableChecker);
                        blindChecked = true;
                    }

                    // check if expired the supposed retry seconds
                    if (timeExpireJudge.isExpired()) {
                        timeExpireJudge.stop();
                        LOG.info("exceed the max retrying seconds {}, stop retrying force", maxRetrySeconds);
                        shouldRetry = false;
                    }

                    if (shouldRetry) {
                        if (null == retryInfo) {
                            retryInfo = new RetryInfo(globalJobType, objectNameShouldBeRetried);
                        }
                        retryInfo.retry(e);
                        long realIntervalMs = retryInfo.getRealRetryIntervalMs(freezeTimeUnit, freezeInternal);
                        LOG.warn("call function {} with {} times failed, try to execute it again according to {}",
                            throwableFunction.toString(), retryInfo.getRetryCount(), blindChecked ? " blind" : "recover-checker", e);
                        freezeTimeUnit.sleep(freezeInternal);

                        if (null != retryInfoConsumerWithExpireTime) {
                            retryInfoConsumerWithExpireTime.accept(retryInfo, Time.now() + realIntervalMs);
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        // reach here, we should throw error
        if ((!(error instanceof Exception)) || (!isFatalError)) {
            error = new FatalDtsException("common", ErrorCode.COMMON_LIB_EXCEED_MAX_RETRY_TIMES, error,
                "retry {} times, {} seconds, which exceed the supposed {} seconds",
                retryTime, timeExpireJudge.getElapsedTime(TimeUnit.SECONDS), maxRetrySeconds);
        }
        throw (Exception) error;
    }

    private boolean determineCanBlindRetry(int retriedTimes,
                                           int blindRetryTimes,
                                           BiFunction<Throwable, Integer, Boolean> recoverableChecker) {
        int adjustedBlinkRetryTimes = blindRetryTimes;
        if (null != recoverableChecker) {
            adjustedBlinkRetryTimes = SwallowException.callAndSwallowException(() -> {
                Throwable detectFakeError = new RecoverableDtsException(
                    "common-lib", ErrorCode.SUCCESS, "fake an exception for detect recoverableChecker", null);
                if (recoverableChecker.apply(detectFakeError, 0)) {
                    return blindRetryTimes;
                }
                return 0;
            });
        }
        return retriedTimes < adjustedBlinkRetryTimes;
    }

    public static class RetryInfo {
        private final String retryModule;
        private final String retryTarget;
        private String errMsg;

        private long retryCount;
        private long beginTimestampMs;
        private long endTimestampMs;
        private long expiredTimestamp;
        private long previousRetryTimestampMs;

        public RetryInfo(String retryModule, String retryTarget) {
            this.retryModule = retryModule;
            this.retryTarget = retryTarget;
            this.retryCount = 0;
            this.errMsg = "null";
            this.beginTimestampMs = 0;
            this.endTimestampMs = 0;
            this.previousRetryTimestampMs = 0;
        }

        public long getRealRetryIntervalMs(TimeUnit freezeTimeUnit, long freezeInternal) {
            long now = Time.now();
            long ret = now - previousRetryTimestampMs;
            previousRetryTimestampMs = now;
            if (retryCount == 1) {
                return freezeTimeUnit.toMillis(freezeInternal);
            } else {
                return ret;
            }
        }

        public long getExpiredTimestamp() {
            return expiredTimestamp;
        }

        public void setExpiredTimestamp(long expiredTimestamp) {
            this.expiredTimestamp = expiredTimestamp;
        }

        public boolean isRetrying() {
            return 0 != beginTimestampMs && 0 == endTimestampMs;
        }

        public long getBeginTimestampMs() {
            return beginTimestampMs;
        }

        void beginRetry() {
            this.beginTimestampMs = Time.now();
        }

        void endRetry() {
            this.endTimestampMs = Time.now();
        }

        public long getRetryCount() {
            return retryCount;
        }

        public void retry(Throwable e) {
            if (0 == beginTimestampMs) {
                beginRetry();
            }
            if (null != e) {
                errMsg = e.toString();
            }
            retryCount++;
        }

        public String getRetryModule() {
            return retryModule;
        }

        public String getErrMsg() {
            return errMsg;
        }

        void setErrMsg(String errMsg) {
            this.errMsg = errMsg;
        }

        public String getRetryTarget() {
            return retryTarget;
        }

        public long getRetryTime(TimeUnit unit) {
            long end = (0 == endTimestampMs) ? Time.now() : endTimestampMs;
            return unit.convert(end - beginTimestampMs, TimeUnit.MILLISECONDS);
        }
    }
}
