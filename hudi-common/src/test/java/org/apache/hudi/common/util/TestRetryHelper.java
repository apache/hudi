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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test retry helper.
 */
public class TestRetryHelper {

  private static final int NUM = 1;
  private static final long INTERVAL_TIME = 1L;

  @Test
  public void testCheckIfExceptionInRetryList() throws Exception {
    // test default retry exceptions
    RetryHelper retryHelper = new RetryHelper(INTERVAL_TIME, NUM, INTERVAL_TIME, "");
    Method privateOne = retryHelper.getClass().getDeclaredMethod("checkIfExceptionInRetryList", Exception.class);
    privateOne.setAccessible(true);
    boolean retry = (boolean) privateOne.invoke(retryHelper, new IOException("test"));
    assertTrue(retry);
    retry = (boolean) privateOne.invoke(retryHelper, new Exception("test"));
    assertFalse(retry);
    // test user-defined retry exceptions
    retryHelper =  new RetryHelper(INTERVAL_TIME, NUM, INTERVAL_TIME, Exception.class.getName());
    retry = (boolean) privateOne.invoke(retryHelper, new UnsupportedOperationException("test"));
    assertTrue(retry);
  }

  @Test
  public void testCheckTooManyTimes() {
    int maxRetries = 100;
    RetryHelper retryHelper = new RetryHelper(INTERVAL_TIME, maxRetries, INTERVAL_TIME, null);
    AtomicInteger counter = new AtomicInteger(0);
    assertDoesNotThrow(() -> {
      retryHelper.start(() -> {
        if (counter.incrementAndGet() < maxRetries) {
          throw new IOException("test");
        }
        return true;
      });
    });
  }
}
