package com.aliyun.dts.deliver.commons.exceptions;

public class DtsCoreException extends DtsExceptionBase {

    private Recoverable recoverable = Recoverable.NO;

    public DtsCoreException(int errorCode) {
        super("framework", errorCode, null, null);
    }

    public DtsCoreException(int errorCode, String message) {
        super("framework", errorCode, message, null);
    }

    public DtsCoreException(int errorCode, String message, Recoverable recoverable) {
        super("framework", errorCode, message, null);
        this.recoverable = recoverable;
    }

    public DtsCoreException(int errorCode, String message, Throwable cause) {
        super("framework", errorCode, message, cause);
    }

    public DtsCoreException(int errorCode, String message, Throwable cause, Recoverable recoverable) {
        super("framework", errorCode, message, cause);
        this.recoverable = recoverable;
    }

    public DtsCoreException(int errorCode, String format, Object... params) {
        super("framework", errorCode, formatErrorMessage(format, params), null);
    }

    @Override
    public Recoverable getRecoverable() {
        return recoverable;
    }

    public void setRecoverable(Recoverable recoverable) {
        this.recoverable = recoverable;
    }
}

