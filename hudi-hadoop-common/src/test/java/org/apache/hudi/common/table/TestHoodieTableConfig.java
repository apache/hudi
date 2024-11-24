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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.RecordMergeMode.CUSTOM;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_CHECKSUM;
import static org.apache.hudi.common.util.ConfigUtils.recoverIfNeeded;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests {@link HoodieTableConfig}.
 */
public class TestHoodieTableConfig extends HoodieCommonTestHarness {

  private HoodieStorage storage;
  private StoragePath metaPath;
  private StoragePath cfgPath;
  private StoragePath backupCfgPath;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, HoodieTestUtils.getDefaultStorageConfWithDefaults());
    metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), "test-table");
    HoodieTableConfig.create(storage, metaPath, props);
    cfgPath = new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    backupCfgPath = new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP);
  }

  @AfterEach
  public void tearDown() throws Exception {
    storage.close();
  }

  @Test
  public void testCreate() throws IOException {
    assertTrue(
        storage.exists(new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE)));
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(7, config.getProps().size());
  }

  @Test
  public void testUpdate() throws IOException {
    Properties updatedProps = new Properties();
    updatedProps.setProperty(HoodieTableConfig.NAME.key(), "test-table2");
    updatedProps.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "new_field");
    HoodieTableConfig.update(storage, metaPath, updatedProps);

    assertTrue(storage.exists(cfgPath));
    assertFalse(storage.exists(backupCfgPath));
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(8, config.getProps().size());
    assertEquals("test-table2", config.getTableName());
    assertEquals("new_field", config.getPreCombineField());
  }

  @Test
  public void testDelete() throws IOException {
    Set<String> deletedProps = CollectionUtils.createSet(HoodieTableConfig.TIMELINE_HISTORY_PATH.key(),
        "hoodie.invalid.config");
    HoodieTableConfig.delete(storage, metaPath, deletedProps);

    assertTrue(storage.exists(cfgPath));
    assertFalse(storage.exists(backupCfgPath));
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(6, config.getProps().size());
    assertNull(config.getProps().getProperty("hoodie.invalid.config"));
    assertFalse(config.getProps().contains(HoodieTableConfig.TIMELINE_HISTORY_PATH.key()));
  }

  @Test
  public void testReadsWhenPropsFileDoesNotExist() throws IOException {
    storage.deleteFile(cfgPath);
    assertThrows(HoodieIOException.class, () -> {
      new HoodieTableConfig(storage, metaPath, null, null, null);
    });
  }

  @Test
  public void testReadsWithUpdateFailures() throws IOException {
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    storage.deleteFile(cfgPath);
    try (OutputStream out = storage.create(backupCfgPath)) {
      config.getProps().store(out, "");
    }

    assertFalse(storage.exists(cfgPath));
    assertTrue(storage.exists(backupCfgPath));
    config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(7, config.getProps().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testUpdateRecovery(boolean shouldPropsFileExist) throws IOException {
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    if (!shouldPropsFileExist) {
      storage.deleteFile(cfgPath);
    }
    try (OutputStream out = storage.create(backupCfgPath)) {
      config.getProps().store(out, "");
    }

    recoverIfNeeded(storage, cfgPath, backupCfgPath);
    assertTrue(storage.exists(cfgPath));
    assertFalse(storage.exists(backupCfgPath));
    config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(7, config.getProps().size());
  }

  @Test
  public void testReadRetry() throws IOException {
    // When both the hoodie.properties and hoodie.properties.backup do not exist then the read fails
    storage.rename(cfgPath, new StoragePath(cfgPath.toString() + ".bak"));
    assertThrows(HoodieIOException.class, () -> new HoodieTableConfig(storage, metaPath, null, null, null));

    // Should return the backup config if hoodie.properties is not present
    storage.rename(new StoragePath(cfgPath.toString() + ".bak"), backupCfgPath);
    new HoodieTableConfig(storage, metaPath, null, null, null);

    // Should return backup config if hoodie.properties is corrupted
    Properties props = new Properties();
    props.put(TABLE_CHECKSUM.key(), "0");
    try (OutputStream out = storage.create(cfgPath)) {
      props.store(out, "Wrong checksum in file so is invalid");
    }
    new HoodieTableConfig(storage, metaPath, null, null, null);

    // Should throw exception if both hoodie.properties and backup are corrupted
    try (OutputStream out = storage.create(backupCfgPath)) {
      props.store(out, "Wrong checksum in file so is invalid");
    }
    assertThrows(IllegalArgumentException.class, () -> new HoodieTableConfig(storage,
        metaPath, null, null, null));
  }

  @Test
  public void testConcurrentlyUpdate() throws ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    Future updaterFuture = executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        Properties updatedProps = new Properties();
        updatedProps.setProperty(HoodieTableConfig.NAME.key(), "test-table" + i);
        updatedProps.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "new_field" + i);
        HoodieTableConfig.update(storage, metaPath, updatedProps);
      }
    });

    Future readerFuture = executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        // Try to load the table properties, won't throw any exception
        new HoodieTableConfig(storage, metaPath, null, null, null);
      }
    });

    updaterFuture.get();
    readerFuture.get();
    executor.shutdown();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SEVEN", "EIGHT"})
  public void testPartitionFields(HoodieTableVersion version) {
    Properties updatedProps = new Properties();
    updatedProps.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), version.greaterThan(HoodieTableVersion.SEVEN) ? "p1:simple,p2:timestamp" : "p1,p2");
    updatedProps.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    HoodieTableConfig.update(storage, metaPath, updatedProps);

    // Test makes sure that the partition fields returned by table config do not have partition type
    // to ensure backward compatibility for the API
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertArrayEquals(new String[] {"p1", "p2"}, config.getPartitionFields().get());
    assertEquals("p1,p2", config.getPartitionFieldProp());
  }

  @ParameterizedTest
  @ValueSource(strings = {"p1:simple,p2:timestamp", "p1,p2"})
  public void testPartitionFieldAPIs(String partitionFields) {
    Properties updatedProps = new Properties();
    updatedProps.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), partitionFields);
    HoodieTableConfig.update(storage, metaPath, updatedProps);

    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath, null, null, null);
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(config).get());
    assertEquals("p1,p2", HoodieTableConfig.getPartitionFieldProp(config).get());
    assertArrayEquals(Arrays.stream(partitionFields.split(BaseKeyGenerator.FIELD_SEPARATOR)).toArray(), HoodieTableConfig.getPartitionFieldsForKeyGenerator(config).get().toArray());
    assertArrayEquals(new String[] {"p1", "p2"}, HoodieTableConfig.getPartitionFields(config).get());
    assertEquals("p1", HoodieTableConfig.getPartitionFieldWithoutKeyGenPartitionType(partitionFields.split(",")[0], config));
  }

  @Test
  public void testValidateConfigVersion() {
    assertTrue(HoodieTableConfig.validateConfigVersion(HoodieTableConfig.INITIAL_VERSION, HoodieTableVersion.EIGHT));
    assertTrue(HoodieTableConfig.validateConfigVersion(ConfigProperty.key("").noDefaultValue().withDocumentation(""),
        HoodieTableVersion.SIX));
    assertFalse(HoodieTableConfig.validateConfigVersion(HoodieTableConfig.INITIAL_VERSION, HoodieTableVersion.SIX));
  }

  @Test
  public void testDropInvalidConfigs() {
    // test invalid configs are dropped
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.SIX.versionCode()));
    config.setValue(HoodieTableConfig.INITIAL_VERSION, String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    config.setValue(RECORD_MERGE_MODE, RECORD_MERGE_MODE.defaultValue().name());

    HoodieTableConfig.dropInvalidConfigs(config);
    assertTrue(config.contains(HoodieTableConfig.VERSION));
    assertFalse(config.contains(HoodieTableConfig.INITIAL_VERSION));
    assertFalse(config.contains(RECORD_MERGE_MODE));

    // test valid ones are not dropped
    config = new HoodieConfig();
    config.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    config.setValue(RECORD_MERGE_MODE, RECORD_MERGE_MODE.defaultValue().name());
    HoodieTableConfig.dropInvalidConfigs(config);
    assertTrue(config.contains(RECORD_MERGE_MODE));
  }

  @Test
  public void testDefinedTableConfigs() {
    List<ConfigProperty<?>> configProperties = HoodieTableConfig.definedTableConfigs();
    assertEquals(38, configProperties.size());
    configProperties.forEach(c -> {
      assertNotNull(c);
      assertFalse(c.doc().isEmpty());
    });
  }

  private static Stream<Arguments> argumentsForInferringRecordMergeMode() {
    String defaultPayload = DefaultHoodieRecordPayload.class.getName();
    String overwritePayload = OverwriteWithLatestAvroPayload.class.getName();
    String customPayload = "custom_payload";
    String customStrategy = "custom_strategy";

    Stream<Arguments> arguments = Stream.of(
        //test empty args with both null and ""
        arguments(null, null, null, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(null, "", "", false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),

        //test legal event time ordering combos
        arguments(EVENT_TIME_ORDERING, null, null, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(EVENT_TIME_ORDERING, defaultPayload, null, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID, false, EVENT_TIME_ORDERING, defaultPayload,
            DEFAULT_MERGE_STRATEGY_UUID),
        arguments(EVENT_TIME_ORDERING, null, DEFAULT_MERGE_STRATEGY_UUID, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(null, defaultPayload, null, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(null, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),
        arguments(null, null, DEFAULT_MERGE_STRATEGY_UUID, false, EVENT_TIME_ORDERING, defaultPayload, DEFAULT_MERGE_STRATEGY_UUID),

        //test legal overwrite combos
        arguments(COMMIT_TIME_ORDERING, null, null, false, COMMIT_TIME_ORDERING, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(COMMIT_TIME_ORDERING, overwritePayload, null, false, COMMIT_TIME_ORDERING, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(COMMIT_TIME_ORDERING, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, false, COMMIT_TIME_ORDERING, overwritePayload,
            COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(COMMIT_TIME_ORDERING, null, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, false, COMMIT_TIME_ORDERING, overwritePayload,
            COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(null, overwritePayload, null, false, COMMIT_TIME_ORDERING, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(null, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, false, COMMIT_TIME_ORDERING, overwritePayload,
            COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        arguments(null, null, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, false, COMMIT_TIME_ORDERING, overwritePayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),

        //test legal custom payload combos
        arguments(CUSTOM, customPayload, null, false, CUSTOM, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID),
        arguments(CUSTOM, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, false, CUSTOM, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID),
        arguments(null, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, false, CUSTOM, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID),
        arguments(null, customPayload, null, false, CUSTOM, customPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID),

        //test legal custom merger combos
        arguments(CUSTOM, null, customStrategy, false, CUSTOM, defaultPayload, customStrategy),
        //for now this case is ok but will need to be changed when we add dummy payload for [HUDI-8317]
        arguments(CUSTOM, defaultPayload, customStrategy, false, CUSTOM, defaultPayload, customStrategy),

        //test illegal combos due to missing info
        arguments(CUSTOM, null, null, true, null, null, null),
        arguments(CUSTOM, null, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),

        //test illegal combos
        arguments(EVENT_TIME_ORDERING, overwritePayload, null, true, null, null, null),
        arguments(EVENT_TIME_ORDERING, customPayload, null, true, null, null, null),
        arguments(EVENT_TIME_ORDERING, null, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(EVENT_TIME_ORDERING, null, customStrategy, true, null, null, null),
        arguments(EVENT_TIME_ORDERING, null, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(COMMIT_TIME_ORDERING, defaultPayload, null, true, null, null, null),
        arguments(COMMIT_TIME_ORDERING, customPayload, null, true, null, null, null),
        arguments(COMMIT_TIME_ORDERING, null, DEFAULT_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(COMMIT_TIME_ORDERING, null, customStrategy, true, null, null, null),
        arguments(COMMIT_TIME_ORDERING, null, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, defaultPayload, null, true, null, null, null),
        arguments(CUSTOM, overwritePayload, null, true, null, null, null),
        arguments(CUSTOM, null, DEFAULT_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, null, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, defaultPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, overwritePayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, defaultPayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(CUSTOM, overwritePayload, DEFAULT_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(null, defaultPayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(null, overwritePayload, PAYLOAD_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(null, defaultPayload, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, true, null, null, null),
        arguments(null, overwritePayload, DEFAULT_MERGE_STRATEGY_UUID, true, null, null, null));
    return arguments;
  }

  @ParameterizedTest
  @MethodSource("argumentsForInferringRecordMergeMode")
  public void testInferMergeMode(RecordMergeMode inputMergeMode, String inputPayloadClass, String inputMergeStrategy, boolean shouldThrow,
                                 RecordMergeMode outputMergeMode, String outputPayloadClass, String outputMergeStrategy) {
    if (shouldThrow) {
      assertThrows(IllegalArgumentException.class,
          () -> HoodieTableConfig.inferCorrectMergingBehavior(inputMergeMode, inputPayloadClass, inputMergeStrategy));
    } else {
      Triple<RecordMergeMode, String, String> inferredConfigs = HoodieTableConfig.inferCorrectMergingBehavior(inputMergeMode, inputPayloadClass, inputMergeStrategy);
      assertEquals(outputMergeMode, inferredConfigs.getLeft());
      assertEquals(outputPayloadClass, inferredConfigs.getMiddle());
      assertEquals(outputMergeStrategy, inferredConfigs.getRight());
    }
  }
}
