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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class LoggingTestUtils {

  public static class InMemoryLogAppender extends AbstractAppender {
    private final List<LogEvent> logEvents = new LinkedList<>();
    private final Logger logger;

    protected InMemoryLogAppender(Logger logger) {
      super(UUID.randomUUID().toString(), null, null, false, null);
      this.logger = logger;
    }

    @Override
    public void append(LogEvent event) {
      logEvents.add(event);
    }

    public List<LogEvent> getLogEvents() {
      return logEvents;
    }

    public void clear() {
      logEvents.clear();
    }

    @Override
    public void stop() {
      super.stop();
      logger.removeAppender(this);
    }
  }

  public static InMemoryLogAppender attachInMemoryAppender(Class cls) {
    Logger logger = (Logger) LogManager.getLogger(cls.getName().replaceAll("\\$$", ""));
    InMemoryLogAppender appender = new InMemoryLogAppender(logger);
    logger.addAppender(appender);
    appender.start();
    return appender;
  }

  public static Stream<LogEvent> getMatchingLogEvents(String msg, InMemoryLogAppender appender) {
    return appender.getLogEvents().stream().filter(e -> e.getMessage().getFormattedMessage().contains(msg));
  }

  public static String getLogs(InMemoryLogAppender appender) {
    StringBuilder builder = new StringBuilder();
    appender.getLogEvents().forEach(logEvent -> builder.append(logEvent.getMessage().getFormattedMessage()).append("\n"));
    return builder.toString();
  }
}
