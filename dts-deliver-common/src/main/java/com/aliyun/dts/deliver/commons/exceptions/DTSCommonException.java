package com.aliyun.dts.deliver.commons.exceptions;

public class DTSCommonException extends DtsExceptionBase {

    private Recoverable recoverable = Recoverable.NO;

    public DTSCommonException(int errorCode) {
        super("common", errorCode, null, null);
    }

    public DTSCommonException(int errorCode, String message) {
        super("common", errorCode, message, null);
    }

    public DTSCommonException(int errorCode, String message, Recoverable recoverable) {
        super("common", errorCode, message, null);
        this.recoverable = recoverable;
    }

    public DTSCommonException(int errorCode, String message, Throwable cause) {
        super("common", errorCode, message, cause);
    }

    public DTSCommonException(int errorCode, String message, Throwable cause, Recoverable recoverable) {
        super("common", errorCode, message, cause);
        this.recoverable = recoverable;
    }

    @Override
    public Recoverable getRecoverable() {
        return recoverable;
    }

    public void setRecoverable(Recoverable recoverable) {
        this.recoverable = recoverable;
    }
}
