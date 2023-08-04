package com.aliyun.dts.deliver.commons.exceptions;

public class CriticalDtsException extends DtsExceptionBase {

    public CriticalDtsException(String moduleName, int errorCode, String message, Throwable cause) {
        super(moduleName, errorCode, message, cause);
    }

    public CriticalDtsException(String moduleName, int errorCode, String message) {
        super(moduleName, errorCode, message, null);
    }

    public CriticalDtsException(String moduleName, int errorCode, Throwable error) {
        super(moduleName, errorCode,
                null == error ? null : error.getMessage(),
                error);
    }

    public CriticalDtsException(String moduleName, int errorCode, Throwable cause, String format, Object... args) {
        super(moduleName, errorCode, formatErrorMessage(format, args), cause);
    }

    @Override
    public Recoverable getRecoverable() {
        return Recoverable.NO;
    }
}
