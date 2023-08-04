/*
 * Copyright (c) 2022 Dts, Inc., all rights reserved.
 */

package com.aliyun.dts.deliver.commons.concurrency;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface VoidCallable extends Callable<Void> {

  VoidCallable NOOP = () -> {};

  default @Override Void call() throws Exception {
    voidCall();
    return null;
  }

  void voidCall() throws Exception;

}
