/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Retry Helper implementation.
 *
 * @param <T> Type of return value for checked function.
 */
public class RetryHelper<T, R extends Exception> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RetryHelper.class);
  private static final List<? extends Class<? extends Exception>> DEFAULT_RETRY_EXCEPTIONS = Arrays.asList(IOException.class, RuntimeException.class);
  private transient CheckedFunction<T, R> func;
  private final int num;
  private final long maxIntervalTime;
  private final long initialIntervalTime;
  private String taskInfo = "N/A";
  private List<? extends Class<? extends Exception>> retryExceptionsClasses;

  public RetryHelper(long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs,
                     List<Class<? extends Exception>> retryExceptions, String taskInfo) {
    this.num = maxRetryNumbers;
    this.initialIntervalTime = initialRetryIntervalMs;
    this.maxIntervalTime = maxRetryIntervalMs;
    this.retryExceptionsClasses = retryExceptions;
    this.taskInfo = taskInfo;
  }

  public RetryHelper(long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions) {
    this.num = maxRetryNumbers;
    this.initialIntervalTime = initialRetryIntervalMs;
    this.maxIntervalTime = maxRetryIntervalMs;
    if (StringUtils.isNullOrEmpty(retryExceptions)) {
      this.retryExceptionsClasses = DEFAULT_RETRY_EXCEPTIONS;
    } else {
      try {
        this.retryExceptionsClasses = Arrays.stream(retryExceptions.split(","))
            .map(exception -> (Exception) ReflectionUtils.loadClass(exception, ""))
            .map(Exception::getClass)
            .collect(Collectors.toList());
      } catch (HoodieException e) {
        LOG.error("Exception while loading retry exceptions classes '" + retryExceptions + "'.", e);
        this.retryExceptionsClasses = DEFAULT_RETRY_EXCEPTIONS;
      }
    }
  }

  public RetryHelper(long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions, String taskInfo) {
    this(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptions);
    this.taskInfo = taskInfo == null ? "None" : taskInfo;
  }

  public RetryHelper<T, R> tryWith(CheckedFunction<T, R> func) {
    this.func = func;
    return this;
  }

  public T start(CheckedFunction<T, R> func) throws R {
    int retries = 0;
    T functionResult = null;

    while (true) {
      long waitTime = Math.min(getWaitTimeExp(retries), maxIntervalTime);
      try {
        functionResult = func.get();
        break;
      } catch (Exception e) {
        if (!checkIfExceptionInRetryList(e)) {
          throw e;
        }
        if (retries++ >= num) {
          String message = "Still failed to " + taskInfo + " after retrying " + num + " times.";
          LOG.error(message, e);
          throw e;
        }
        LOG.warn("Catch Exception for " + taskInfo + ", current retry number " + retries + ", will retry after " + waitTime + " ms.", e);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException ex) {
          // ignore InterruptedException here
        }
      }
    }

    if (retries > 0) {
      LOG.info("Success to {} after retried {} times.", taskInfo, retries);
    }

    return functionResult;
  }

  public T start() throws R {
    return start(this.func);
  }

  private boolean checkIfExceptionInRetryList(Exception e) {
    boolean inRetryList = false;
    for (Class<? extends Exception> clazz : retryExceptionsClasses) {
      if (clazz.isInstance(e)) {
        inRetryList = true;
        break;
      }
    }
    return inRetryList;
  }

  private long getWaitTimeExp(int retryCount) {
    Random random = new Random();
    if (0 == retryCount) {
      return initialIntervalTime;
    }

    // avoid long type overflow
    long waitTime = (long) Math.pow(2, retryCount) * initialIntervalTime;
    if (waitTime < 0) {
      return Long.MAX_VALUE;
    }
    waitTime += random.nextInt(100);
    return waitTime < 0 ? Long.MAX_VALUE : waitTime;
  }

  /**
   * Checked function interface.
   *
   * @param <T> Type of return value.
   */
  @FunctionalInterface
  public interface CheckedFunction<T, R extends Exception> extends Serializable {
    T get() throws R;
  }
}
