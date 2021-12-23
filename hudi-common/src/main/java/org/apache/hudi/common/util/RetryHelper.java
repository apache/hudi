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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

public class RetryHelper<T> {
  private static final Logger LOG = LogManager.getLogger(RetryHelper.class);
  private CheckedFunction<T> func;
  private int num;
  private long maxIntervalTime;
  private long initialIntervalTime = 100L;
  private String taskInfo = "N/A";

  public RetryHelper() {
  }

  public RetryHelper(String taskInfo) {
    this.taskInfo = taskInfo;
  }

  public RetryHelper tryWith(CheckedFunction<T> func) {
    this.func = func;
    return this;
  }

  public RetryHelper tryNum(int num) {
    this.num = num;
    return this;
  }

  public RetryHelper tryTaskInfo(String taskInfo) {
    this.taskInfo = taskInfo;
    return this;
  }

  public RetryHelper tryMaxInterval(long time) {
    maxIntervalTime = time;
    return this;
  }

  public RetryHelper tryInitialInterval(long time) {
    initialIntervalTime = time;
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
        if (retries++ >= num) {
          LOG.error("Still failed to " + taskInfo + " after retried " + num + " times.", e);
          throw e;
        }
        LOG.warn("Catch Exception " + taskInfo + ", will retry after " + waitTime + " ms.", e);
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

  private long getWaitTimeExp(int retryCount) {
    Random random = new Random();
    if (0 == retryCount) {
      return initialIntervalTime;
    }

    return (long) Math.pow(2, retryCount) * initialIntervalTime + random.nextInt(100);
  }

  @FunctionalInterface
  public interface CheckedFunction<T> {
    T get() throws IOException;
  }
}