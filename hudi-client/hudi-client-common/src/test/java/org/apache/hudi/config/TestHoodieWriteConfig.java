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

package org.apache.hudi.config;

import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieWriteConfig.Builder;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.hudi.config.HoodieArchivalConfig.ASYNC_ARCHIVE;
import static org.apache.hudi.config.HoodieCleanConfig.ASYNC_CLEAN;
import static org.apache.hudi.config.HoodieCleanConfig.AUTO_CLEAN;
import static org.apache.hudi.config.HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT;
import static org.apache.hudi.config.HoodieWriteConfig.TABLE_SERVICES_ENABLED;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_CONCURRENCY_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieWriteConfig {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPropertyLoading(boolean withAlternative) throws IOException {
    Builder builder = HoodieWriteConfig.newBuilder().withPath("/tmp");
    Map<String, String> params = new HashMap<>(3);
    params.put(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "1");
    params.put(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "5");
    params.put(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2");
    if (withAlternative) {
      params.put("hoodie.avro.schema.externalTransformation", "true");
    } else {
      params.put("hoodie.avro.schema.external.transformation", "true");
    }
    ByteArrayOutputStream outStream = saveParamsIntoOutputStream(params);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    try {
      builder = builder.fromInputStream(inputStream);
    } finally {
      outStream.close();
      inputStream.close();
    }
    HoodieWriteConfig config = builder.build();
    assertEquals(5, config.getMaxCommitsToKeep());
    assertEquals(2, config.getMinCommitsToKeep());
    assertTrue(config.shouldUseExternalSchemaTransformation());
  }

  @Test
  public void testDefaultIndexAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getIndexType,
        constructConfigMap(
            EngineType.SPARK, HoodieIndex.IndexType.SIMPLE,
            EngineType.FLINK, HoodieIndex.IndexType.INMEMORY,
            EngineType.JAVA, HoodieIndex.IndexType.INMEMORY));
  }

  @Test
  public void testDefaultClusteringPlanStrategyClassAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getClusteringPlanStrategyClass,
        constructConfigMap(
            EngineType.SPARK, HoodieClusteringConfig.SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY,
            EngineType.FLINK, HoodieClusteringConfig.FLINK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY,
            EngineType.JAVA, HoodieClusteringConfig.JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY));
  }

  @Test
  public void testDefaultClusteringExecutionStrategyClassAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getClusteringExecutionStrategyClass,
        constructConfigMap(
            EngineType.SPARK, HoodieClusteringConfig.SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY,
            EngineType.FLINK, HoodieClusteringConfig.JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY,
            EngineType.JAVA, HoodieClusteringConfig.JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY));
  }

  @Test
  public void testDefaultMarkersTypeAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getMarkersType,
        constructConfigMap(
            EngineType.SPARK, MarkerType.TIMELINE_SERVER_BASED,
            EngineType.FLINK, MarkerType.DIRECT,
            EngineType.JAVA, MarkerType.DIRECT));
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testAutoConcurrencyConfigAdjustmentWithTableServices(HoodieTableType tableType) {
    final String inProcessLockProviderClassName = InProcessLockProvider.class.getCanonicalName();
    // With metadata table enabled by default, any async table service enabled should
    // use InProcess lock provider as default when no other lock provider is set.
    // 1. Async clustering
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "true");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "false");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, true, WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieFailedWritesCleaningPolicy.LAZY, inProcessLockProviderClassName);

    // 2. Async clean
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "false");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "true");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, true, WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieFailedWritesCleaningPolicy.LAZY, inProcessLockProviderClassName);

    // 3. Async compaction configured
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "false");
            put(INLINE_COMPACT.key(), "false");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "false");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true,
        tableType == HoodieTableType.MERGE_ON_READ, true,
        tableType == HoodieTableType.MERGE_ON_READ
            ? WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL
            : WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
        tableType == HoodieTableType.MERGE_ON_READ
            ? HoodieFailedWritesCleaningPolicy.LAZY
            : HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        tableType == HoodieTableType.MERGE_ON_READ
            ? inProcessLockProviderClassName
            : HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());

    // 4. All inline services
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "false");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "false");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, false, true,
        WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());

    // 5. All async services
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "true");
            put(INLINE_COMPACT.key(), "false");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "true");
            put(ASYNC_ARCHIVE.key(), "true");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, false,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieFailedWritesCleaningPolicy.LAZY, inProcessLockProviderClassName);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testAutoAdjustLockConfigs(HoodieTableType tableType) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieTableConfig.TYPE.key(), tableType.name());
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withAutoAdjustLockConfigs(false)
        .withClusteringConfig(new HoodieClusteringConfig.Builder().withAsyncClustering(true).build())
        .withProperties(properties)
        .build();

    verifyConcurrencyControlRelatedConfigs(writeConfig,
        true, true, true,
        WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());

    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withAutoAdjustLockConfigs(false)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withClusteringConfig(new HoodieClusteringConfig.Builder().withAsyncClustering(true).build())
        .withProperties(properties)
        .build();

    verifyConcurrencyControlRelatedConfigs(writeConfig,
        true, true, true,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL, HoodieFailedWritesCleaningPolicy.LAZY,
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testAutoConcurrencyConfigAdjustmentWithUserConfigs(HoodieTableType tableType) {
    // 1. User override for the lock provider should always take the precedence
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieTableConfig.TYPE.key(), tableType.name());
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProviderTestClass.class)
            .build())
        .withAutoAdjustLockConfigs(true)
        .withProperties(properties)
        .build();

    verifyConcurrencyControlRelatedConfigs(writeConfig,
        true, tableType == HoodieTableType.MERGE_ON_READ, true,
        WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        FileSystemBasedLockProviderTestClass.class.getName());

    // 2. User can set the lock provider via properties
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(ASYNC_CLUSTERING_ENABLE.key(), "false");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "true");
            put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
                ZookeeperBasedLockProvider.class.getName());
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, true,
        WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        ZookeeperBasedLockProvider.class.getName());

    // 3. Default config should have default lock provider
    writeConfig = createWriteConfig(new HashMap<String, String>() {
      {
        put(HoodieTableConfig.TYPE.key(), tableType.name());
        put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
      }
    });
    if (writeConfig.areAnyTableServicesAsync()) {
      verifyConcurrencyControlRelatedConfigs(writeConfig,
          true, true, true,
          WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
          HoodieFailedWritesCleaningPolicy.LAZY,
          InProcessLockProvider.class.getName());
    } else {
      verifyConcurrencyControlRelatedConfigs(writeConfig,
          true, false, true,
          WriteConcurrencyMode.valueOf(WRITE_CONCURRENCY_MODE.defaultValue()),
          HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
          HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testAutoConcurrencyConfigAdjustmentWithNoTableService(HoodieTableType tableType) {
    // 1. No table service, concurrency control configs should not be overwritten
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(TABLE_SERVICES_ENABLED.key(), "false");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), false, false, false,
        WriteConcurrencyMode.fromValue(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());

    // 2. No table service, with optimistic concurrency control,
    // failed write clean policy should be updated accordingly
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(TABLE_SERVICES_ENABLED.key(), "false");
            put(WRITE_CONCURRENCY_MODE.key(),
                WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value());
            put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
                FileSystemBasedLockProviderTestClass.class.getName());
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), false, false, false,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieFailedWritesCleaningPolicy.LAZY,
        FileSystemBasedLockProviderTestClass.class.getName());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testAutoConcurrencyConfigAdjustmentWithMetadataTableDisabled(HoodieTableType tableType) {
    // 1. Metadata table disabled, with async table services, concurrency control configs
    // should not be changed
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(HoodieTableConfig.TYPE.key(), tableType.name());
            put(HoodieMetadataConfig.ENABLE.key(), "false");
            put(ASYNC_CLUSTERING_ENABLE.key(), "true");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "false");
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, true,
        WriteConcurrencyMode.fromValue(WRITE_CONCURRENCY_MODE.defaultValue()),
        HoodieFailedWritesCleaningPolicy.valueOf(FAILED_WRITES_CLEANER_POLICY.defaultValue()),
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.defaultValue());

    // 2. Metadata table disabled, with optimistic concurrency control,
    // failed write clean policy should be updated accordingly
    verifyConcurrencyControlRelatedConfigs(createWriteConfig(new HashMap<String, String>() {
          {
            put(ASYNC_CLUSTERING_ENABLE.key(), "true");
            put(INLINE_COMPACT.key(), "true");
            put(AUTO_CLEAN.key(), "true");
            put(ASYNC_CLEAN.key(), "false");
            put(WRITE_CONCURRENCY_MODE.key(),
                WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.value());
            put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
                FileSystemBasedLockProviderTestClass.class.getName());
            put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
          }
        }), true, true, true,
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        HoodieFailedWritesCleaningPolicy.LAZY, FileSystemBasedLockProviderTestClass.class.getName());
  }

  @Test
  public void testConsistentBucketIndexDefaultClusteringConfig() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING).build())
        .build();
    assertEquals(HoodieClusteringConfig.SPARK_CONSISTENT_BUCKET_CLUSTERING_PLAN_STRATEGY, writeConfig.getClusteringPlanStrategyClass());
    assertEquals(HoodieClusteringConfig.SPARK_CONSISTENT_BUCKET_EXECUTION_STRATEGY, writeConfig.getClusteringExecutionStrategyClass());
  }

  @Test
  public void testConsistentBucketIndexInvalidClusteringConfig() {
    TypedProperties consistentBucketIndexProps = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
        .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING).build().getProps();
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder().withPath("/tmp");

    assertThrows(IllegalArgumentException.class,
        () -> writeConfigBuilder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .fromProperties(consistentBucketIndexProps)
            .withClusteringPlanStrategyClass(HoodieClusteringConfig.JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY).build()));
    assertThrows(IllegalArgumentException.class,
        () -> writeConfigBuilder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .fromProperties(consistentBucketIndexProps)
            .withClusteringExecutionStrategyClass(HoodieClusteringConfig.SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY).build()));
  }

  @Test
  public void testSimpleBucketIndexPartitionerConfig() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE).build())
        .build();
    assertEquals(HoodieLayoutConfig.SIMPLE_BUCKET_LAYOUT_PARTITIONER_CLASS_NAME, writeConfig.getString(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME));

    HoodieWriteConfig overwritePartitioner = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder().withLayoutPartitioner("org.apache.hudi.table.action.commit.UpsertPartitioner").build())
        .build();
    assertEquals("org.apache.hudi.table.action.commit.UpsertPartitioner", overwritePartitioner.getString(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME));
  }

  private HoodieWriteConfig createWriteConfig(Map<String, String> configs) {
    final Properties properties = new Properties();
    configs.forEach(properties::setProperty);
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withProperties(properties)
        .build();
  }

  private ByteArrayOutputStream saveParamsIntoOutputStream(Map<String, String> params) throws IOException {
    Properties properties = new Properties();
    properties.putAll(params);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    properties.store(outStream, "Saved on " + new Date(System.currentTimeMillis()));
    return outStream;
  }

  /**
   * Tests the engine-specific configuration values for one configuration key .
   *
   * @param getConfigFunc     Function to get the config value.
   * @param expectedConfigMap Expected config map, with key as the engine type
   *                          and value as the corresponding config value for the engine.
   */
  private void testEngineSpecificConfig(Function<HoodieWriteConfig, Object> getConfigFunc,
                                        Map<EngineType, Object> expectedConfigMap) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").build();
    assertEquals(expectedConfigMap.get(EngineType.SPARK), getConfigFunc.apply(writeConfig));

    for (EngineType engineType : expectedConfigMap.keySet()) {
      writeConfig = HoodieWriteConfig.newBuilder()
          .withEngineType(engineType).withPath("/tmp").build();
      assertEquals(expectedConfigMap.get(engineType), getConfigFunc.apply(writeConfig));
    }
  }

  /**
   * Constructs the map.
   *
   * @param k1 First engine type.
   * @param v1 Config value for the first engine type.
   * @param k2 Second engine type.
   * @param v2 Config value for the second engine type.
   * @param k3 Third engine type.
   * @param v3 Config value for the third engine type.
   * @return {@link Map<EngineType, Object>} instance, with key as the engine type
   * and value as the corresponding config value for the engine.
   */
  private Map<EngineType, Object> constructConfigMap(
      EngineType k1, Object v1, EngineType k2, Object v2, EngineType k3, Object v3) {
    Map<EngineType, Object> mapping = new HashMap<>();
    mapping.put(k1, v1);
    mapping.put(k2, v2);
    mapping.put(k3, v3);
    return mapping;
  }

  private void verifyConcurrencyControlRelatedConfigs(
      HoodieWriteConfig writeConfig, boolean expectedTableServicesEnabled,
      boolean expectedAnyTableServicesAsync,
      boolean expectedAnyTableServicesExecutedInline,
      WriteConcurrencyMode expectedConcurrencyMode,
      HoodieFailedWritesCleaningPolicy expectedCleanPolicy,
      String expectedLockProviderName) {
    assertEquals(expectedTableServicesEnabled, writeConfig.areTableServicesEnabled());
    assertEquals(expectedAnyTableServicesAsync, writeConfig.areAnyTableServicesAsync());
    assertEquals(
        expectedAnyTableServicesExecutedInline, writeConfig.areAnyTableServicesExecutedInline());
    assertEquals(expectedConcurrencyMode, writeConfig.getWriteConcurrencyMode());
    assertEquals(expectedCleanPolicy, writeConfig.getFailedWritesCleanPolicy());
    assertEquals(expectedLockProviderName, writeConfig.getLockProviderClass());
  }
}
