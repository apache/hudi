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

package org.apache.hudi.utilities;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.MetadataTableServiceMode;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMetadataTableServicesTool {

  @Test
  void warnsWhenScheduleModeSkipsCleanAndArchive() {
    Logger logger = (Logger) LogManager.getLogger(HoodieMetadataTableServicesTool.class);
    Level originalLevel = logger.getLevel();
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender = new AbstractAppender("metadata-services-test", null, null, false, null) {
      @Override
      public void append(LogEvent event) {
        events.add(event.toImmutable());
      }
    };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.WARN);
    try {
      HoodieMetadataTableServicesTool.validateRequest(MetadataTableServiceMode.SCHEDULE,
          EnumSet.of(TableServiceType.COMPACT, TableServiceType.CLEAN, TableServiceType.ARCHIVE), null);
      assertEquals(1, events.size());
      assertEquals(Level.WARN, events.get(0).getLevel());
      String message = events.get(0).getMessage().getFormattedMessage();
      assertTrue(message.contains("CLEAN"));
      assertTrue(message.contains("ARCHIVE"));
      assertTrue(message.contains("schedule-only"));
      events.clear();
      HoodieMetadataTableServicesTool.validateRequest(MetadataTableServiceMode.SCHEDULE,
          EnumSet.of(TableServiceType.COMPACT), null);
      HoodieMetadataTableServicesTool.validateRequest(MetadataTableServiceMode.EXECUTE,
          EnumSet.of(TableServiceType.CLEAN, TableServiceType.ARCHIVE), null);
      assertTrue(events.isEmpty());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(originalLevel);
      appender.stop();
    }
  }

  @Test
  void parsesAllAndSelectedServices() {
    assertEquals(EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT,
        TableServiceType.CLEAN, TableServiceType.ARCHIVE),
        HoodieMetadataTableServicesTool.parseServices("all"));
    assertEquals(EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT),
        HoodieMetadataTableServicesTool.parseServices("compaction,log-compaction"));
  }

  @Test
  void rejectsUnsupportedServices() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableServicesTool.parseServices("clustering"));
  }

  @Test
  void validatesScheduleRequests() {
    assertThrows(IllegalArgumentException.class, () ->
        HoodieMetadataTableServicesTool.validateRequest(
            MetadataTableServiceMode.SCHEDULE, EnumSet.of(TableServiceType.CLEAN), null));
    assertDoesNotThrow(() ->
        HoodieMetadataTableServicesTool.validateRequest(
            MetadataTableServiceMode.SCHEDULE, EnumSet.of(TableServiceType.COMPACT), null));
  }

  @Test
  void validatesInstantRequests() {
    for (TableServiceType service : EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT)) {
      assertDoesNotThrow(() -> HoodieMetadataTableServicesTool.validateRequest(
          MetadataTableServiceMode.EXECUTE, EnumSet.of(service), "instant"));
      for (MetadataTableServiceMode mode : EnumSet.of(
          MetadataTableServiceMode.SCHEDULE, MetadataTableServiceMode.SCHEDULE_AND_EXECUTE)) {
        assertThrows(IllegalArgumentException.class, () -> HoodieMetadataTableServicesTool.validateRequest(
            mode, EnumSet.of(service), "instant"));
      }
      assertThrows(IllegalArgumentException.class, () -> HoodieMetadataTableServicesTool.validateRequest(
          MetadataTableServiceMode.EXECUTE, EnumSet.of(service, TableServiceType.CLEAN), "instant"));
    }
    assertThrows(IllegalArgumentException.class, () -> HoodieMetadataTableServicesTool.validateRequest(
        MetadataTableServiceMode.EXECUTE, EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT), "instant"));
    for (TableServiceType service : EnumSet.of(TableServiceType.CLEAN, TableServiceType.ARCHIVE)) {
      assertThrows(IllegalArgumentException.class, () -> HoodieMetadataTableServicesTool.validateRequest(
          MetadataTableServiceMode.EXECUTE, EnumSet.of(service), "instant"));
    }
  }

  @Test
  void rejectsEmptyServiceRequests() {
    for (MetadataTableServiceMode mode : MetadataTableServiceMode.values()) {
      assertThrows(IllegalArgumentException.class, () -> HoodieMetadataTableServicesTool.validateRequest(
          mode, EnumSet.noneOf(TableServiceType.class), null));
    }
  }

  @Test
  void validatesSharedDataTableLockConfiguration() {
    HoodieWriteConfig singleWriterConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/data-table")
        .build();
    assertThrows(HoodieException.class, () ->
        HoodieMetadataTableServicesTool.validateDataTableLockConfiguration(singleWriterConfig));

    HoodieWriteConfig occConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/data-table")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withFileSystemLockPath("/tmp/data-table-lock")
            .build())
        .build();
    assertDoesNotThrow(() ->
        HoodieMetadataTableServicesTool.validateDataTableLockConfiguration(occConfig));
  }
}
