package com.aliyun.dts.deliver.commons.exceptions;

public class RecoverableDtsException extends DtsExceptionBase {

    public RecoverableDtsException(String moduleName, int errorCode, String message, Throwable cause) {
        super(moduleName, errorCode, message, cause);
    }

    public RecoverableDtsException(String moduleName, int errorCode, Throwable cause, String format, Object... args) {
        super(moduleName, errorCode, formatErrorMessage(format, args), cause);
    }

    @Override
    public Recoverable getRecoverable() {
        return Recoverable.YES;
    }
}
