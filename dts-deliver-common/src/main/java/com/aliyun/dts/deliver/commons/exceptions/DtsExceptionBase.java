package com.aliyun.dts.deliver.commons.exceptions;

import org.apache.commons.lang3.StringUtils;

public abstract class DtsExceptionBase extends RuntimeException {

        private static final String ERROR_PREFIX = "DTS-";
        private static final String FIELD_SEPARATOR = ": ";
        private final int errorCode;
        private final String moduleName;

    public DtsExceptionBase(String moduleName, int errorCode, String message, Throwable cause) {
            super(message, cause);

            this.moduleName = moduleName;
            this.errorCode = errorCode;
        }

        public abstract Recoverable getRecoverable();

        @Override
        public String getMessage() {
            String errorCodeString = String.format("%05d", errorCode);
            String message = ERROR_PREFIX + errorCodeString + FIELD_SEPARATOR + super.getMessage();

            if (!StringUtils.isEmpty(moduleName)) {
                message = moduleName + FIELD_SEPARATOR + message;
            }

            return message;
        }

        public String getAllExceptionMessage() {
            StringBuilder sbl = new StringBuilder(getMessage());
            Throwable cause = this.getCause();
            if (null != cause) {
                sbl.append(System.lineSeparator());
                sbl.append("cause:");
            }
            while (null != cause) {
                sbl.append(System.lineSeparator());
                sbl.append("\t").append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage());
                cause = cause.getCause();
            }

            return sbl.toString();
        }

        public int getErrorCode() {
            return errorCode;
        }

        public enum Recoverable {
            IGNORE, // the exception can be ignored
            YES,    // the exception can be recovered
            NO,     // the exception can not be recovered
            FATAL,  // the exception is fatal, we should stop immediately
            UNKNOWN
        }

        @Override
        public String toString() {
            return getAllExceptionMessage();
        }

        protected static final String formatErrorMessage(String format, Object... args) {
            StringBuilder sbl = new StringBuilder(format.length() + 64);
            boolean meetOpenCurly = false;
            int markLength = 0;
            int argIndex = 0;

            if (null == args) {
                args = new Object[0];
            }
            for (char fmtChar : format.toCharArray()) {
                if ('{' == fmtChar) {
                    meetOpenCurly = true;
                    markLength = sbl.length();
                } else if ('}' == fmtChar) {
                    if (meetOpenCurly) {
                        // now we meet the closed curly, try replace it with
                        sbl.setLength(markLength);
                        if (argIndex < args.length) {
                            sbl.append(args[argIndex++]);
                        }
                        continue;
                    }
                }
                sbl.append(fmtChar);
            }
            return sbl.toString();
        }
}
