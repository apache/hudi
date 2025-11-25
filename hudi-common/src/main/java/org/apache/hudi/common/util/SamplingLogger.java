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

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A sampling logger that logs at INFO level once every N times, otherwise at DEBUG level.
 * This is useful for reducing log volume while still maintaining periodic visibility
 * into frequently executed code paths.
 */
public class SamplingLogger {

  private final Logger logger;
  private final int sampleFrequency;
  private final AtomicInteger counter;

  /**
   * Creates a new SamplingLogger.
   *
   * @param logger          the underlying SLF4J logger to use
   * @param sampleFrequency log at INFO level once every N calls (e.g., 5 means every 5th call)
   */
  public SamplingLogger(Logger logger, int sampleFrequency) {
    this.logger = logger;
    this.sampleFrequency = sampleFrequency;
    this.counter = new AtomicInteger(0);
  }

  /**
   * Returns true if the next log call should be at INFO level based on sampling frequency.
   * This method increments the internal counter.
   *
   * @return true if the caller should log at INFO level, false for DEBUG level
   */
  public boolean shouldLogAtInfo() {
    return counter.incrementAndGet() % sampleFrequency == 0;
  }

  /**
   * Logs a message at INFO level if it's time to sample, otherwise at DEBUG level.
   *
   * @param message the log message (can contain {} placeholders)
   * @param args    the arguments to substitute into the message
   */
  public void logInfoOrDebug(String message, Object... args) {
    if (shouldLogAtInfo()) {
      logger.info(message, args);
    } else {
      logger.debug(message, args);
    }
  }
}
