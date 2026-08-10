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
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A sampling logger that logs at INFO level once every N times, otherwise at DEBUG level.
 * This is useful for reducing log volume while still maintaining periodic visibility
 * into frequently executed code paths.
 */
public class SamplingLogger implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String loggerName;
  private transient Logger logger;
  private final int sampleFrequency;
  private final AtomicInteger counter;

  /**
   * Creates a new SamplingLogger.
   *
   * @param logger          the underlying SLF4J logger to use
   * @param sampleFrequency log at INFO level once every N calls (e.g., 5 means every 5th call)
   */
  public SamplingLogger(Logger logger, int sampleFrequency) {
    this.loggerName = logger.getName();
    this.logger = logger;
    this.sampleFrequency = sampleFrequency;
    this.counter = new AtomicInteger(0);
  }

  /**
   * Gets the logger, recreating it if necessary after deserialization.
   */
  private Logger getLogger() {
    if (logger == null) {
      logger = LoggerFactory.getLogger(loggerName);
    }
    return logger;
  }

  /**
   * Returns true if the next log call should be at INFO level based on sampling frequency.
   * This method increments the internal counter.
   *
   * @return true if the caller should log at INFO level, false for DEBUG level
   */
  @VisibleForTesting
  boolean shouldLogAtInfo() {
    return counter.incrementAndGet() % sampleFrequency == 0;
  }

  /**
   * Logs a message at INFO level if it's time to sample, otherwise at DEBUG level.
   * Uses a Supplier to lazily evaluate arguments, avoiding computation when logging is disabled.
   *
   * @param message      the log message (can contain {} placeholders)
   * @param argsSupplier a supplier that provides the arguments to substitute into the message
   */
  public void logInfoOrDebug(String message, Supplier<Object[]> argsSupplier) {
    if (shouldLogAtInfo()) {
      getLogger().info(message, argsSupplier.get());
    } else if (getLogger().isDebugEnabled()) {
      getLogger().debug(message, argsSupplier.get());
    }
  }
}