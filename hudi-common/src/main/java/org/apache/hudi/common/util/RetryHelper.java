/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class RetryHelper<T> implements Serializable {
  private static final Logger LOG = LogManager.getLogger(RetryHelper.class);
  private transient CheckedFunction<T> func;
  private final int num;
  private final long maxIntervalTime;
  private final long initialIntervalTime;
  private String taskInfo = "N/A";
  private List<? extends Class<? extends Exception>> retryExceptionsClasses;

  public RetryHelper(long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions) {
    this.num = maxRetryNumbers;
    this.initialIntervalTime = initialRetryIntervalMs;
    this.maxIntervalTime = maxRetryIntervalMs;
    if (StringUtils.isNullOrEmpty(retryExceptions)) {
      this.retryExceptionsClasses = new ArrayList<>();
    } else {
      try {
        this.retryExceptionsClasses = Arrays.stream(retryExceptions.split(","))
                .map(exception -> (Exception) ReflectionUtils.loadClass(exception, ""))
                .map(Exception::getClass)
                .collect(Collectors.toList());
      } catch (HoodieException e) {
        LOG.error("Exception while loading retry exceptions classes '" + retryExceptions + "'.", e);
        this.retryExceptionsClasses = new ArrayList<>();
      }
    }
  }

  public RetryHelper(long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions, String taskInfo) {
    this(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptions);
    this.taskInfo = taskInfo;
  }

  public RetryHelper<T> tryWith(CheckedFunction<T> func) {
    this.func = func;
    return this;
  }

  public T start() throws IOException {
    int retries = 0;
    T functionResult = null;

    while (true) {
      long waitTime = Math.min(getWaitTimeExp(retries), maxIntervalTime);
      try {
        functionResult = func.get();
        break;
      } catch (IOException | RuntimeException e) {
        if (!checkIfExceptionInRetryList(e)) {
          throw e;
        }
        if (retries++ >= num) {
          String message = "Still failed to " + taskInfo + " after retried " + num + " times.";
          LOG.error(message, e);
          if (e instanceof IOException) {
            throw new IOException(message, e);
          }
          throw e;
        }
        LOG.warn("Catch Exception for " + taskInfo + ", will retry after " + waitTime + " ms.", e);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException ex) {
          // ignore InterruptedException here
        }
      }
    }

    if (retries > 0) {
      LOG.info("Success to " + taskInfo + " after retried " + retries + " times.");
    }

    return functionResult;
  }

  private boolean checkIfExceptionInRetryList(Exception e) {
    boolean inRetryList = false;

    // if users didn't set hoodie.filesystem.operation.retry.exceptions
    // we will retry all the IOException and RuntimeException
    if (retryExceptionsClasses.isEmpty()) {
      return true;
    }

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

    return (long) Math.pow(2, retryCount) * initialIntervalTime + random.nextInt(100);
  }

  @FunctionalInterface
  public interface CheckedFunction<T> extends Serializable {
    T get() throws IOException;
  }
}