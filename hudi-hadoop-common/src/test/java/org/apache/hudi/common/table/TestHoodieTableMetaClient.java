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

package org.apache.hudi.common.table;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.RecordPayloadType;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests hoodie table meta client {@link HoodieTableMetaClient}.
 */
public class TestHoodieTableMetaClient extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void checkMetadata() {
    assertEquals(HoodieTestUtils.RAW_TRIPS_TEST_NAME, metaClient.getTableConfig().getTableName(),
        "Table name should be raw_trips");
    assertEquals(basePath, metaClient.getBasePath().toString(), "Basepath should be the one assigned");
    assertEquals(basePath + "/.hoodie", metaClient.getMetaPath().toString(),
        "Metapath should be ${basepath}/.hoodie");
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertTrue(HoodieTableConfig.validateChecksum(metaClient.getTableConfig().getProps()));
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig().getTableInitialVersion());
  }

  @Test
  public void checkSerDe() {
    // check if this object is serialized and de-serialized, we are able to read from the file system
    HoodieTableMetaClient deserializedMetaClient =
        HoodieTestUtils.serializeDeserialize(metaClient, HoodieTableMetaClient.class);
    assertNotNull(deserializedMetaClient);
    HoodieActiveTimeline commitTimeline = deserializedMetaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    commitTimeline.createNewInstant(instant);
    commitTimeline.saveAsComplete(instant, Option.of(getUTF8Bytes("test-detail")));
    commitTimeline = commitTimeline.reload();
    HoodieInstant completedInstant = commitTimeline.getInstantsAsStream().findFirst().get();
    assertTrue(completedInstant.isCompleted());
    assertEquals(completedInstant.getTimestamp(), instant.getTimestamp());
    assertArrayEquals(getUTF8Bytes("test-detail"), commitTimeline.getInstantDetails(completedInstant).get(),
        "Commit value should be \"test-detail\"");
  }

  @Test
  public void checkCommitTimeline() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertTrue(activeCommitTimeline.empty(), "Should be empty commit timeline");

    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(getUTF8Bytes("test-detail")));

    // Commit timeline should not auto-reload every time getActiveCommitTimeline(), it should be cached
    activeTimeline = metaClient.getActiveTimeline();
    activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertTrue(activeCommitTimeline.empty(), "Should be empty commit timeline");

    activeTimeline = activeTimeline.reload();
    HoodieInstant completedInstant = activeTimeline.getCommitsTimeline().getInstantsAsStream().findFirst().get();
    activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertFalse(activeCommitTimeline.empty(), "Should be the 1 commit we made");
    assertTrue(completedInstant.isCompleted());
    assertTrue(completedInstant.getTimestamp().equals(instant.getTimestamp()));
    assertArrayEquals(getUTF8Bytes("test-detail"), activeCommitTimeline.getInstantDetails(completedInstant).get(),
        "Commit value should be \"test-detail\"");
  }

  private static Stream<Arguments> argumentsForInferringRecordMergeMode() {
    Stream<Arguments> arguments = Stream.of(
        // Record merger strategy is not set
        // Payload class is set, payload type is not set
        arguments(Option.of(OverwriteWithLatestAvroPayload.class.getName()),
            Option.empty(), Option.empty(), RecordMergeMode.OVERWRITE_WITH_LATEST),
        arguments(Option.of(DefaultHoodieRecordPayload.class.getName()),
            Option.empty(), Option.empty(), RecordMergeMode.EVENT_TIME_ORDERING),
        arguments(Option.of(PostgresDebeziumAvroPayload.class.getName()),
            Option.empty(), Option.empty(), RecordMergeMode.CUSTOM),
        // Record merger strategy is not set
        // Payload class is set, payload type is set; payload class takes precedence
        arguments(Option.of(OverwriteWithLatestAvroPayload.class.getName()),
            Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.empty(), RecordMergeMode.OVERWRITE_WITH_LATEST),
        arguments(Option.of(DefaultHoodieRecordPayload.class.getName()),
            Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.empty(), RecordMergeMode.EVENT_TIME_ORDERING),
        arguments(Option.of(PostgresDebeziumAvroPayload.class.getName()),
            Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.empty(), RecordMergeMode.CUSTOM),
        // Record merger strategy is set to default
        // Payload class is set, payload type is not set
        arguments(Option.of(OverwriteWithLatestAvroPayload.class.getName()),
            Option.empty(), Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.OVERWRITE_WITH_LATEST),
        arguments(Option.of(DefaultHoodieRecordPayload.class.getName()),
            Option.empty(), Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.EVENT_TIME_ORDERING),
        arguments(Option.of(PostgresDebeziumAvroPayload.class.getName()),
            Option.empty(), Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.CUSTOM),
        // Record merger strategy is set to default
        // Payload class is not set, payload type is set
        arguments(Option.empty(), Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.OVERWRITE_WITH_LATEST),
        arguments(Option.empty(), Option.of(RecordPayloadType.HOODIE_AVRO_DEFAULT.name()),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.EVENT_TIME_ORDERING),
        arguments(Option.empty(), Option.of(RecordPayloadType.HOODIE_METADATA.name()),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.CUSTOM),
        // Record merger strategy is set to default
        // Payload class or payload type is not set
        arguments(Option.empty(), Option.empty(), Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.valueOf(RECORD_MERGE_MODE.defaultValue())),
        // Record merger strategy is set to custom
        arguments(Option.empty(), Option.empty(), Option.of("custom_merge_strategy"),
            RecordMergeMode.CUSTOM),
        arguments(Option.of(DefaultHoodieRecordPayload.class.getName()),
            Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.of("custom_merge_strategy"), RecordMergeMode.CUSTOM)
    );
    return arguments;
  }

  @ParameterizedTest
  @MethodSource("argumentsForInferringRecordMergeMode")
  public void testInferRecordMergeMode(Option<String> payloadClassName,
                                       Option<String> payloadType,
                                       Option<String> recordMergerStrategy,
                                       RecordMergeMode expectedRecordMergeMode) {
    HoodieTableMetaClient.PropertyBuilder builder = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ.name())
        .setTableName("table_name");
    if (payloadClassName.isPresent()) {
      builder.setPayloadClassName(payloadClassName.get());
    }
    if (payloadType.isPresent()) {
      builder.setPayloadType(payloadType.get());
    }
    if (recordMergerStrategy.isPresent()) {
      builder.setRecordMergerStrategy(recordMergerStrategy.get());
    }
    assertEquals(expectedRecordMergeMode,
        RecordMergeMode.valueOf(builder.build().getProperty(RECORD_MERGE_MODE.key())));
  }

  private static Stream<Arguments> argumentsForValidationFailureOnMergeConfigs() {
    Stream<Arguments> arguments = Stream.of(
        arguments(Option.of(DefaultHoodieRecordPayload.class.getName()), Option.empty(),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.OVERWRITE_WITH_LATEST,
            "Payload class name (org.apache.hudi.common.model.DefaultHoodieRecordPayload) or type "
                + "(null) should be consistent with the record merge mode OVERWRITE_WITH_LATEST"),
        arguments(Option.empty(), Option.of(RecordPayloadType.HOODIE_AVRO_DEFAULT.name()),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.OVERWRITE_WITH_LATEST,
            "Payload class name (null) or type (HOODIE_AVRO_DEFAULT) "
                + "should be consistent with the record merge mode OVERWRITE_WITH_LATEST"),
        arguments(Option.of(OverwriteWithLatestAvroPayload.class.getName()), Option.empty(),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.EVENT_TIME_ORDERING,
            "Payload class name (org.apache.hudi.common.model.OverwriteWithLatestAvroPayload) or type "
                + "(null) should be consistent with the record merge mode EVENT_TIME_ORDERING"),
        arguments(Option.empty(), Option.of(RecordPayloadType.OVERWRITE_LATEST_AVRO.name()),
            Option.of(DEFAULT_MERGER_STRATEGY_UUID),
            RecordMergeMode.EVENT_TIME_ORDERING,
            "Payload class name (null) or type (OVERWRITE_LATEST_AVRO) "
                + "should be consistent with the record merge mode EVENT_TIME_ORDERING")
    );
    return arguments;
  }

  @ParameterizedTest
  @MethodSource("argumentsForValidationFailureOnMergeConfigs")
  public void testValidationFailureOnMergeConfigs(Option<String> payloadClassName,
                                                  Option<String> payloadType,
                                                  Option<String> recordMergerStrategy,
                                                  RecordMergeMode recordMergeMode,
                                                  String expectedErrorMessage) {
    HoodieTableMetaClient.PropertyBuilder builder = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ.name())
        .setTableName("table_name")
        .setRecordMergeMode(recordMergeMode);
    if (payloadClassName.isPresent()) {
      builder.setPayloadClassName(payloadClassName.get());
    }
    if (payloadType.isPresent()) {
      builder.setPayloadType(payloadType.get());
    }
    if (recordMergerStrategy.isPresent()) {
      builder.setRecordMergerStrategy(recordMergerStrategy.get());
    }
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class, builder::build);
    assertEquals(expectedErrorMessage, exception.getMessage());
  }

  @Test
  public void testEquals() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1, metaClient1);
    assertEquals(metaClient1, metaClient2);
    assertNotEquals(metaClient1, null);
    assertNotEquals(metaClient1, new Object());
  }

  @Test
  public void testToString() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1.toString(), metaClient2.toString());
    assertNotEquals(metaClient1.toString(), new Object().toString());
  }
}
