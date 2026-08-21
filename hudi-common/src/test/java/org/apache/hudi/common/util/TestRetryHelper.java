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

import org.apache.hudi.common.testutils.HoodieTestLogAppender;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

  /**
   * The point of HUDI-9095: a retry that is going to be attempted again must not dump a stack trace
   * into the log. The cause still has to be identifiable from the message itself.
   */
  @Test
  public void testRetryWarningCarriesNoStackTrace() {
    HoodieTestLogAppender appender = new HoodieTestLogAppender().attachTo(RetryHelper.class);
    try {
      AtomicInteger attempts = new AtomicInteger(0);
      RetryHelper retryHelper = new RetryHelper(INTERVAL_TIME, 3, INTERVAL_TIME, (String) null, "save partition metafile");
      assertDoesNotThrow(() -> retryHelper.start(() -> {
        if (attempts.incrementAndGet() < 3) {
          throw new IOException("Failed to create file /a/b/.hoodie_partition_metadata",
              new FileAlreadyExistsException("File already exists: /a/b/.hoodie_partition_metadata"));
        }
        return true;
      }));

      List<LogEvent> warnings = appender.getLog().stream()
          .filter(event -> Level.WARN.equals(event.getLevel())).collect(Collectors.toList());
      assertFalse(warnings.isEmpty(), "the retries should still be reported at warn level");
      for (LogEvent warning : warnings) {
        assertNull(warning.getThrown(),
            "the retry warning must not carry a throwable, otherwise the logger prints its stack trace");
        String message = warning.getMessage().getFormattedMessage();
        assertTrue(message.contains("save partition metafile"), message);
        assertTrue(message.contains("java.io.IOException: Failed to create file /a/b/.hoodie_partition_metadata"), message);
        assertTrue(message.contains("FileAlreadyExistsException"), "the root cause must survive: " + message);
      }
    } finally {
      appender.detach();
    }
  }

  @Test
  public void testSummarizeKeepsRootCauseOnASingleLine() {
    // a wrapped exception keeps both layers, so the warning stays actionable without a stack trace
    IOException wrapped = new IOException("Failed to create file /a/b/.hoodie_partition_metadata",
        new FileAlreadyExistsException("File already exists: /a/b/.hoodie_partition_metadata"));
    String summary = RetryHelper.summarize(wrapped);
    assertTrue(summary.contains("java.io.IOException: Failed to create file /a/b/.hoodie_partition_metadata"), summary);
    assertTrue(summary.contains("caused by java.nio.file.FileAlreadyExistsException"), summary);
    assertFalse(summary.contains("\n"), "the summary must stay on a single line: " + summary);
    assertFalse(summary.contains("\tat "), "the summary must not carry a stack trace: " + summary);

    // an exception with no cause is rendered as-is
    assertEquals("java.io.IOException: plain", RetryHelper.summarize(new IOException("plain")));
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
