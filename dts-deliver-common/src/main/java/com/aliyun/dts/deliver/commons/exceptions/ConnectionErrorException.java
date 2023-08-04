/*
 * Copyright (c) 2022 Dts, Inc., all rights reserved.
 */

package com.aliyun.dts.deliver.commons.exceptions;

public class ConnectionErrorException extends RuntimeException {

  private String stateCode;
  private int errorCode;
  private String exceptionMessage;

  public ConnectionErrorException(final String exceptionMessage) {
    super(exceptionMessage);
  }

  public ConnectionErrorException(final String stateCode, final Throwable exception) {
    super(exception);
    this.stateCode = stateCode;
    this.exceptionMessage = exception.getMessage();
  }

  public ConnectionErrorException(final String stateCode,
                                  final String exceptionMessage,
                                  final Throwable exception) {
    super(exception);
    this.stateCode = stateCode;
    this.exceptionMessage = exceptionMessage;
  }

  public ConnectionErrorException(final String stateCode,
                                  final int errorCode,
                                  final String exceptionMessage,
                                  final Throwable exception) {
    super(exception);
    this.stateCode = stateCode;
    this.errorCode = errorCode;
    this.exceptionMessage = exceptionMessage;
  }

  public String getStateCode() {
    return this.stateCode;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getExceptionMessage() {
    return exceptionMessage;
  }

}
