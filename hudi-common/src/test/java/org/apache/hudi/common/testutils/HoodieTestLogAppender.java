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

package org.apache.hudi.common.testutils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Collects the log events a logger emits, so a test can assert on what was logged.
 * <p>
 * Attach it around the code under test and detach it afterwards:
 * <pre>{@code
 * HoodieTestLogAppender appender = new HoodieTestLogAppender();
 * appender.attachTo(SomeClass.class);
 * try {
 *   // exercise the code
 * } finally {
 *   appender.detach();
 * }
 * }</pre>
 * Named so that surefire does not mistake it for a test class.
 */
public class HoodieTestLogAppender extends AbstractAppender {

  private final List<LogEvent> log = new ArrayList<>();
  private Logger attachedLogger;

  public HoodieTestLogAppender() {
    super(UUID.randomUUID().toString(), null, null, false, null);
  }

  @Override
  public void append(LogEvent event) {
    // the event is mutable and gets reused, so keep an immutable copy
    log.add(event.toImmutable());
  }

  public List<LogEvent> getLog() {
    return new ArrayList<>(log);
  }

  /**
   * Starts this appender and attaches it to the logger of the given class.
   */
  public HoodieTestLogAppender attachTo(Class<?> clazz) {
    attachedLogger = (Logger) LogManager.getLogger(clazz);
    start();
    attachedLogger.addAppender(this);
    return this;
  }

  /**
   * Detaches from the logger it was attached to and stops. Safe to call if never attached.
   */
  public void detach() {
    if (attachedLogger != null) {
      attachedLogger.removeAppender(this);
      attachedLogger = null;
    }
    stop();
  }
}
