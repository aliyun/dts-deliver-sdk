package com.aliyun.dts.deliver.commons.exceptions;

import org.apache.commons.lang3.StringUtils;

public enum ExceptionUtil {
    INSTANCE;

    public boolean isFatalException(Throwable e) {
        boolean rs = false;

        while (true) {
            if (null == e) {
                break;
            }

            if (e instanceof DtsExceptionBase) {
                if (DtsExceptionBase.Recoverable.FATAL == ((DtsExceptionBase) e).getRecoverable()) {
                    rs = true;
                    break;
                }
            }

            // OutOfMemoryError should not retry
            String errMsg = e.getMessage();
            if (!StringUtils.isEmpty(errMsg) && (errMsg.contains("Java heap space") || errMsg.contains("GC overhead limit exceeded"))) {
                rs = true;
                break;
            }

            e = e.getCause();
        }

        return rs;
    }
}
