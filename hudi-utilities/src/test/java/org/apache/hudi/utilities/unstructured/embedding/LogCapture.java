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

package org.apache.hudi.utilities.unstructured.embedding;

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
 * Captures the events one class logs while it is open: each as its level, its formatted message and, when it carries one, its
 * throwable's string. Closing it restores the logger.
 */
public final class LogCapture extends AbstractAppender implements AutoCloseable {

  private final List<String> lines = Collections.synchronizedList(new ArrayList<>());
  private final Logger logger;
  private final Level previousLevel;

  private LogCapture(Class<?> source) {
    super(UUID.randomUUID().toString(), null, null, false, null);
    start();
    logger = (Logger) LogManager.getLogger(source);
    previousLevel = logger.getLevel();
    // the appender first: adding it rebuilds the logger from the active config, which would reset an earlier level
    logger.addAppender(this);
    logger.setLevel(Level.ALL);
  }

  /** Starts capturing what {@code source}'s logger logs. */
  public static LogCapture of(Class<?> source) {
    return new LogCapture(source);
  }

  @Override
  public void append(LogEvent event) {
    lines.add(event.getLevel() + " " + event.getMessage().getFormattedMessage()
        + (event.getThrown() == null ? "" : " " + event.getThrown()));
  }

  /** Every event captured so far, as {@code LEVEL message}. */
  public List<String> lines() {
    synchronized (lines) {
      return new ArrayList<>(lines);
    }
  }

  /** The WARN events captured so far. */
  public List<String> warnings() {
    List<String> warnings = new ArrayList<>();
    synchronized (lines) {
      for (String line : lines) {
        if (line.startsWith(Level.WARN + " ")) {
          warnings.add(line);
        }
      }
    }
    return warnings;
  }

  @Override
  public void close() {
    logger.removeAppender(this);
    logger.setLevel(previousLevel);
    stop();
  }
}
