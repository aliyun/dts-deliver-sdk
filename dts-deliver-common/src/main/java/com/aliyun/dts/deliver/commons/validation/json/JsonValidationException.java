/*
 * Copyright (c) 2022 Dts, Inc., all rights reserved.
 */

package com.aliyun.dts.deliver.commons.validation.json;

public class JsonValidationException extends Exception {

  public JsonValidationException(final String message) {
    super(message);
  }

  public JsonValidationException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
