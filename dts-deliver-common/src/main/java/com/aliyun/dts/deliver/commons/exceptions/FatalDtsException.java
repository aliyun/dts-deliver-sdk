package com.aliyun.dts.deliver.commons.exceptions;

public class FatalDtsException extends DtsExceptionBase {
    public FatalDtsException(String moduleName, int errorCode, String message, Throwable cause) {
        super(moduleName, errorCode, message, cause);
    }

    public FatalDtsException(String moduleName, int errorCode, String message) {
        super(moduleName, errorCode, message, null);
    }

    public FatalDtsException(String moduleName, int errorCode, Throwable cause, String format, Object... args) {
        super(moduleName, errorCode, formatErrorMessage(format, args), cause);
    }

    @Override
    public Recoverable getRecoverable() {
        return Recoverable.FATAL;
    }
}
