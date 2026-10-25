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

package org.apache.hudi.utilities.testutils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Collects the messages one class logs, for tools whose only output is their log.
 *
 * <p>{@code addAppender} gives the named logger a {@link org.apache.logging.log4j.core.config.LoggerConfig} of
 * its own, cloned from the nearest configured ancestor, and refreshes every live logger from it. That refresh
 * drops any level set beforehand, so the level is raised only after the appender is in place. Events of child
 * loggers still reach the appender through that config, hence the logger name check in
 * {@link #append(LogEvent)}.
 */
public class CapturingLogAppender extends AbstractAppender implements AutoCloseable {

  private final String loggerName;
  private final Logger logger;
  private final Level previousLevel;
  private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

  private CapturingLogAppender(String loggerName, Level level) {
    super("Capture-" + loggerName + "-" + UUID.randomUUID(), null, null, false, null);
    this.loggerName = loggerName;
    this.logger = (Logger) LogManager.getLogger(loggerName);
    this.previousLevel = logger.getLevel();
    start();
    logger.addAppender(this);
    logger.setLevel(level);
  }

  /**
   * Starts capturing everything {@code loggerClass} logs at INFO or above, until the returned appender is closed.
   */
  public static CapturingLogAppender attachTo(Class<?> loggerClass) {
    return new CapturingLogAppender(loggerClass.getName(), Level.INFO);
  }

  @Override
  public void append(LogEvent event) {
    if (loggerName.equals(event.getLoggerName())) {
      messages.add(event.getMessage().getFormattedMessage());
    }
  }

  /**
   * The formatted messages captured so far, in the order they were logged.
   */
  public List<String> messages() {
    return new ArrayList<>(messages);
  }

  @Override
  public void close() {
    logger.removeAppender(this);
    logger.setLevel(previousLevel);
    stop();
  }
}
