/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.buffer.BufferMemoryType;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link OptionsResolver}
 */
public class TestOptionsResolver {
  @TempDir
  File tempFile;

  @Test
  void testGetIndexType() {
    Configuration conf = getConf();
    // set uppercase index
    conf.set(FlinkOptions.INDEX_TYPE, "BLOOM");
    assertEquals(HoodieIndex.IndexType.BLOOM, OptionsResolver.getIndexType(conf));
    // set lowercase index
    conf.set(FlinkOptions.INDEX_TYPE, "bloom");
    assertEquals(HoodieIndex.IndexType.BLOOM, OptionsResolver.getIndexType(conf));
  }

  @Test
  void testGetRecordKeys() {
    Configuration conf = new Configuration();
    assertNull(OptionsResolver.getRecordKeyStr(conf));
    assertArrayEquals(new String[]{}, OptionsResolver.getRecordKeys(conf));

    conf.set(FlinkOptions.RECORD_KEY_FIELD, "");
    assertArrayEquals(new String[]{}, OptionsResolver.getRecordKeys(conf));

    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid, name");
    assertArrayEquals(new String[]{"uuid", "name"}, OptionsResolver.getRecordKeys(conf));
  }

  @Test
  void testGetBucketIndexKeys() {
    Configuration conf = new Configuration();
    assertArrayEquals(new String[]{}, OptionsResolver.getBucketIndexKeys(conf));

    conf.set(FlinkOptions.INDEX_KEY_FIELD, "");
    assertArrayEquals(new String[]{}, OptionsResolver.getBucketIndexKeys(conf));

    conf.set(FlinkOptions.INDEX_KEY_FIELD, "uuid, name");
    assertArrayEquals(new String[]{"uuid", "name"}, OptionsResolver.getBucketIndexKeys(conf));
  }

  @Test
  void testRecordLevelIndexStreamingWrite() {
    Configuration conf = getConf();
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());

    assertTrue(OptionsResolver.isRecordLevelIndex(conf));
    assertTrue(OptionsResolver.isStreamingIndexWriteEnabled(conf));

    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    assertFalse(OptionsResolver.isStreamingIndexWriteEnabled(conf));

    conf.set(FlinkOptions.OPERATION, WriteOperationType.UPSERT.value());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    assertFalse(OptionsResolver.isRecordLevelIndex(conf));
    assertFalse(OptionsResolver.isStreamingIndexWriteEnabled(conf));
  }

  @Test
  void testIsLazyFailedWritesCleanPolicy() {
    Configuration conf = new Configuration();
    // add any parameter
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    // add value for FAILED_WRITES_CLEANER_POLICY using default key
    conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.NEVER.name());
    assertFalse(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));

    if (!HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.getAlternatives().isEmpty()) {
      conf = new Configuration();
      // add any parameter
      conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
      // add value for FAILED_WRITES_CLEANER_POLICY using alternative key
      conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.getAlternatives().get(0), HoodieFailedWritesCleaningPolicy.LAZY.name());
      assertTrue(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));
    }
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    return conf;
  }

  @Test
  void testAreTableServicesEnabled() {
    Configuration conf = new Configuration();
    // default value should be true
    assertTrue(OptionsResolver.areTableServicesEnabled(conf));

    // explicitly set to true
    conf.set(FlinkOptions.TABLE_SERVICES_ENABLED, true);
    assertTrue(OptionsResolver.areTableServicesEnabled(conf));

    // explicitly set to false
    conf.set(FlinkOptions.TABLE_SERVICES_ENABLED, false);
    assertFalse(OptionsResolver.areTableServicesEnabled(conf));
  }

  @Test
  void testTableServicesGateCompactionAndCleaning() {
    Configuration conf = getConf();
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.LAZY.name());

    assertTrue(OptionsResolver.needsAsyncCompaction(conf));
    assertTrue(OptionsResolver.needsScheduleCompaction(conf));
    assertTrue(OptionsResolver.needsAsyncCleaning(conf));
    assertTrue(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));
    assertTrue(OptionsResolver.isLazyFailedWritesCleaning(conf));

    conf.set(FlinkOptions.TABLE_SERVICES_ENABLED, false);

    assertFalse(OptionsResolver.needsAsyncCompaction(conf));
    assertFalse(OptionsResolver.needsScheduleCompaction(conf));
    assertFalse(OptionsResolver.needsAsyncCleaning(conf));
    assertTrue(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));
    assertFalse(OptionsResolver.isLazyFailedWritesCleaning(conf));
  }

  @Test
  void testTableServicesGateMetadataCompaction() {
    Configuration conf = getConf();
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());

    assertTrue(OptionsResolver.needsAsyncMetadataCompaction(conf));
    assertTrue(OptionsResolver.needsScheduleMdtCompaction(conf));

    conf.set(FlinkOptions.TABLE_SERVICES_ENABLED, false);

    assertFalse(OptionsResolver.needsAsyncMetadataCompaction(conf));
    assertFalse(OptionsResolver.needsScheduleMdtCompaction(conf));
  }

  @Test
  void testTableServicesGateClustering() {
    Configuration conf = getConf();
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);

    assertTrue(OptionsResolver.needsAsyncClustering(conf));
    assertTrue(OptionsResolver.needsScheduleClustering(conf));

    conf.set(FlinkOptions.TABLE_SERVICES_ENABLED, false);

    assertFalse(OptionsResolver.needsAsyncClustering(conf));
    assertFalse(OptionsResolver.needsScheduleClustering(conf));
  }

  @Test
  void testEstimateFileGroupCountForPartitionedRLI() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());
    conf.setString(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

    // testing default value
    assertEquals(1, OptionsResolver.estimateFileGroupCountForRLI(conf));

    // testing user configured value
    conf.setString(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "3");
    conf.setString(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "3");
    assertEquals(3, OptionsResolver.estimateFileGroupCountForRLI(conf));
  }

  @Test
  void testEstimateFileGroupCountForGlobalRLI() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

    // testing default value
    assertEquals(8, OptionsResolver.estimateFileGroupCountForRLI(conf));

    // testing user configured value
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "11");
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "11");
    assertEquals(11, OptionsResolver.estimateFileGroupCountForRLI(conf));
  }

  @Test
  void testIncrementalJobGraphPredicate() {
    Configuration conf = new Configuration();
    assertFalse(OptionsResolver.isIncrementalJobGraph(conf));
    conf.set(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION, true);
    assertTrue(OptionsResolver.isIncrementalJobGraph(conf));
  }

  @Test
  void testTableTypePredicates() {
    Configuration conf = new Configuration();
    assertTrue(OptionsResolver.isCowTable(conf));
    assertFalse(OptionsResolver.isMorTable(conf));
    assertFalse(OptionsResolver.isMorTable(Collections.emptyMap()));
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name().toLowerCase());
    assertTrue(OptionsResolver.isMorTable(conf));
    assertTrue(OptionsResolver.isMorTable(
        Collections.singletonMap(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name())));
  }

  @Test
  void testOperationTypePredicates() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    assertTrue(OptionsResolver.isInsertOperation(conf));
    conf.set(FlinkOptions.OPERATION, WriteOperationType.UPSERT.value());
    assertTrue(OptionsResolver.isUpsertOperation(conf));
    conf.set(FlinkOptions.OPERATION, WriteOperationType.BULK_INSERT.value());
    assertTrue(OptionsResolver.isBulkInsertOperation(conf));
  }

  @Test
  void testPayloadAndCompactionPredicates() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, DefaultHoodieRecordPayload.class.getName());
    assertTrue(OptionsResolver.isDefaultHoodieRecordPayloadClazz(conf));
    conf.set(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.TIME_ELAPSED.toUpperCase());
    assertTrue(OptionsResolver.isDeltaTimeCompaction(conf));
    conf.set(FlinkOptions.COMPACTION_TRIGGER_STRATEGY, FlinkOptions.NUM_COMMITS);
    assertFalse(OptionsResolver.isDeltaTimeCompaction(conf));
  }

  @Test
  void testReadCommitsLimit() {
    Configuration conf = new Configuration();
    assertEquals(-1, OptionsResolver.getReadCommitsLimit(conf));
    conf.set(FlinkOptions.READ_COMMITS_LIMIT, 5);
    assertEquals(5, OptionsResolver.getReadCommitsLimit(conf));
  }

  @Test
  void testCdcOptions() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE,
        HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER.name().toLowerCase());
    assertEquals(HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER,
        OptionsResolver.getCDCSupplementalLoggingMode(conf));

    conf.set(FlinkOptions.READ_CDC_FROM_CHANGELOG, false);
    assertFalse(OptionsResolver.readCDCFromChangelog(conf));
  }

  @Test
  void testIndexKeyFields() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id,tenant");
    assertEquals("id", OptionsResolver.getIndexKeyFields(conf).get(0));
    assertEquals("tenant", OptionsResolver.getIndexKeyFields(conf).get(1));
  }

  @Test
  void testSchemaAndTimestampOptions() {
    Configuration conf = new Configuration();
    conf.setString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    assertTrue(OptionsResolver.isSchemaEvolutionEnabled(conf));
    conf.setString(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");
    assertTrue(OptionsResolver.isConsistentLogicalTimestampEnabled(conf));
    conf.setString(HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(),
        HollowCommitHandling.USE_TRANSITION_TIME.name());
    assertTrue(OptionsResolver.isReadByTxnCompletionTime(conf));
  }

  @Test
  void testWriteFlags() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), "true");
    assertTrue(OptionsResolver.allowCommitOnEmptyBatch(conf));
    conf.setString(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), "true");
    assertTrue(OptionsResolver.useComplexKeygenNewEncoding(conf));
  }

  @Test
  void testConcurrencyControlModes() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    assertTrue(OptionsResolver.isNonBlockingConcurrencyControl(conf));
    // The OPTIMISTIC_CONCURRENCY_CONTROL assertion is omitted: OptionsResolver
    // #isOptimisticConcurrencyControl arrives with 348e7f13a7fb (#18946) and does not exist here.
  }

  @Test
  void testInsertPartitioner() {
    Configuration conf = new Configuration();
    assertFalse(OptionsResolver.getInsertPartitioner(conf).isPresent());
    conf.set(FlinkOptions.INSERT_PARTITIONER_CLASS_NAME, TestPartitioner.class.getName());
    assertTrue(OptionsResolver.getInsertPartitioner(conf).isPresent());
    conf.set(FlinkOptions.INSERT_PARTITIONER_CLASS_NAME, String.class.getName());
    assertThrows(HoodieException.class, () -> OptionsResolver.getInsertPartitioner(conf));
  }

  @Test
  void testConflictResolutionStrategies() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BLOOM.name());
    assertInstanceOf(SimpleConcurrentFileWritesConflictResolutionStrategy.class,
        OptionsResolver.getConflictResolutionStrategy(conf));
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    assertInstanceOf(BucketIndexConcurrentFileWritesConflictResolutionStrategy.class,
        OptionsResolver.getConflictResolutionStrategy(conf));
  }

  @Test
  void testWriteBufferSizingAndManagedMemory() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 300D);
    conf.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, 50);
    assertEquals(150L * 1024 * 1024, OptionsResolver.getWriteBufferSizeInBytes(conf));
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 100D);
    assertThrows(IllegalStateException.class, () -> OptionsResolver.getWriteBufferSizeInBytes(conf));

    conf.set(FlinkOptions.WRITE_BUFFER_MEMORY_TYPE, BufferMemoryType.MANAGED.name().toLowerCase());
    assertTrue(OptionsResolver.isManagedMemoryBufferEnabled(conf));
  }

  public static class TestPartitioner implements Partitioner<String> {
    public TestPartitioner(Configuration conf) {
    }

    @Override
    public int partition(String key, int numPartitions) {
      return 0;
    }
  }
}
