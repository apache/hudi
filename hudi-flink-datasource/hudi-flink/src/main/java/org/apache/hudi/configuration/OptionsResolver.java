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

import org.apache.hudi.client.clustering.plan.strategy.FlinkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.overwrite.PartitionOverwriteMode;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.HoodieFlinkIOFactory;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;

/**
 * Tool helping to resolve the flink options {@link FlinkOptions}.
 */
public class OptionsResolver {

  /**
   * Returns whether the current runtime mode is adaptive batch execution.
   */
  public static boolean isIncrementalJobGraph(Configuration conf) {
    return conf.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION);
  }

  /**
   * Returns whether insert clustering is allowed with given configuration {@code conf}.
   */
  public static boolean insertClustering(Configuration conf) {
    return isCowTable(conf) && isInsertOperation(conf) && conf.get(FlinkOptions.INSERT_CLUSTER);
  }

  /**
   * Returns whether the insert is clustering disabled with given configuration {@code conf}.
   */
  public static boolean isAppendMode(Configuration conf) {
    // 1. inline clustering is supported for COW table;
    // 2. async clustering is supported for both COW and MOR table
    return isInsertOperation(conf) && ((isCowTable(conf) && !conf.get(FlinkOptions.INSERT_CLUSTER)) || isMorTable(conf));
  }

  /**
   * Returns whether current index is partition level simple bucket index based on given configuration {@code conf}.
   */
  public static Boolean isPartitionLevelSimpleBucketIndex(Configuration conf) {
    HoodieIndex.BucketIndexEngineType engineType = OptionsResolver.getBucketEngineType(conf);
    return engineType.equals(HoodieIndex.BucketIndexEngineType.SIMPLE)
        && PartitionBucketIndexUtils.isPartitionSimpleBucketIndex(HadoopConfigurations.getHadoopConf(conf), conf.get(FlinkOptions.PATH));
  }

  /**
   * Returns whether the table operation is 'insert'.
   */
  public static boolean isInsertOperation(Configuration conf) {
    WriteOperationType operationType = WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION));
    return operationType == WriteOperationType.INSERT;
  }

  /**
   * Returns whether the table operation is 'upsert'.
   */
  public static boolean isUpsertOperation(Configuration conf) {
    WriteOperationType operationType = WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION));
    return operationType == WriteOperationType.UPSERT;
  }

  /**
   * Returns whether the table operation is 'bulk_insert'.
   */
  public static boolean isBulkInsertOperation(Configuration conf) {
    WriteOperationType operationType = WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION));
    return operationType == WriteOperationType.BULK_INSERT;
  }

  /**
   * Returns whether it is a MERGE_ON_READ table.
   */
  public static boolean isMorTable(Configuration conf) {
    return conf.get(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
  }

  /**
   * Returns whether it is a MERGE_ON_READ table.
   */
  public static boolean isMorTable(Map<String, String> options) {
    return options.getOrDefault(FlinkOptions.TABLE_TYPE.key(),
        FlinkOptions.TABLE_TYPE.defaultValue()).equalsIgnoreCase(FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
  }

  /**
   * Returns whether it is a COPY_ON_WRITE table.
   */
  public static boolean isCowTable(Configuration conf) {
    return conf.get(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
  }

  /**
   * Returns whether the payload clazz is {@link DefaultHoodieRecordPayload}.
   */
  public static boolean isDefaultHoodieRecordPayloadClazz(Configuration conf) {
    return conf.get(FlinkOptions.PAYLOAD_CLASS_NAME).contains(DefaultHoodieRecordPayload.class.getSimpleName());
  }

  /**
   * Returns the ordering fields as comma separated string
   * or null if the value is set as {@link FlinkOptions#NO_PRE_COMBINE}.
   */
  public static String getOrderingFieldsStr(Configuration conf) {
    final String orderingFields = conf.get(FlinkOptions.ORDERING_FIELDS);
    return orderingFields.equals(FlinkOptions.NO_PRE_COMBINE) ? null : orderingFields;
  }

  /**
   * Returns whether the compaction strategy is based on elapsed delta time.
   */
  public static boolean isDeltaTimeCompaction(Configuration conf) {
    final String strategy = conf.get(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toLowerCase(Locale.ROOT);
    return FlinkOptions.TIME_ELAPSED.equals(strategy) || FlinkOptions.NUM_OR_TIME.equals(strategy);
  }

  /**
   * Returns whether the table is partitioned.
   */
  public static boolean isPartitionedTable(Configuration conf) {
    return FilePathUtils.extractPartitionKeys(conf).length > 0;
  }

  /**
   * Returns whether the table index is bucket index.
   */
  public static boolean isBucketIndexType(Configuration conf) {
    return conf.get(FlinkOptions.INDEX_TYPE).equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name());
  }

  /**
   * Returns whether it is a MERGE_ON_READ table, and updates by bucket index.
   */
  public static boolean isMorWithBucketIndexUpsert(Configuration conf) {
    return isMorTable(conf) && isUpsertOperation(conf) && isBucketIndexType(conf);
  }

  public static HoodieIndex.BucketIndexEngineType getBucketEngineType(Configuration conf) {
    String bucketEngineType = conf.get(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE);
    return HoodieIndex.BucketIndexEngineType.valueOf(bucketEngineType);
  }

  /**
   * Returns whether the table index is consistent bucket index.
   */
  public static boolean isConsistentHashingBucketIndexType(Configuration conf) {
    return isBucketIndexType(conf) && getBucketEngineType(conf).equals(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING);
  }

  /**
   * Returns whether the table index is simple bucket index.
   */
  public static boolean isSimpleBucketIndexType(Configuration conf) {
    return isBucketIndexType(conf) && getBucketEngineType(conf).equals(HoodieIndex.BucketIndexEngineType.SIMPLE);
  }

  /**
   * Returns the default plan strategy class.
   */
  public static String getDefaultPlanStrategyClassName(Configuration conf) {
    return OptionsResolver.isConsistentHashingBucketIndexType(conf) ? FlinkConsistentBucketClusteringPlanStrategy.class.getName() :
        FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS.defaultValue();
  }

  /**
   * Returns whether the source should emit changelog.
   *
   * @return true if the source should emit changes.
   */
  public static boolean emitChangelog(Configuration conf) {
    return conf.get(FlinkOptions.READ_AS_STREAMING) && conf.get(FlinkOptions.CHANGELOG_ENABLED)
        || conf.get(FlinkOptions.READ_AS_STREAMING) && conf.get(FlinkOptions.CDC_ENABLED)
        || isIncrementalQuery(conf) && conf.get(FlinkOptions.CDC_ENABLED);
  }

  /**
   * Returns whether the source should emit deletes.
   *
   * @return true if the source is read as streaming with changelog mode enabled.
   */
  public static boolean emitDeletes(Configuration conf) {
    return conf.get(FlinkOptions.READ_AS_STREAMING) && conf.get(FlinkOptions.CHANGELOG_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the async compaction.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsAsyncCompaction(Configuration conf) {
    return OptionsResolver.isMorTable(conf)
        && conf.get(FlinkOptions.COMPACTION_ASYNC_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the compaction plan.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleCompaction(Configuration conf) {
    return OptionsResolver.isMorTable(conf)
        && conf.get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED) && !isAppendMode(conf);
  }

  /**
   * Returns whether there is need to schedule the async clustering.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsAsyncClustering(Configuration conf) {
    return isInsertOperation(conf) && conf.get(FlinkOptions.CLUSTERING_ASYNC_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the clustering plan.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleClustering(Configuration conf) {
    if (!conf.get(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED)) {
      return false;
    }
    WriteOperationType operationType = WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION));
    if (OptionsResolver.isConsistentHashingBucketIndexType(conf)) {
      // Write pipelines for table with consistent bucket index would detect whether clustering service occurs,
      // and automatically adjust the partitioner and write function if clustering service happens.
      // So it could handle UPSERT.
      // But it could not handle INSERT case, because insert write would not take index into consideration currently.
      return operationType == WriteOperationType.UPSERT;
    } else {
      return operationType == WriteOperationType.INSERT;
    }
  }

  /**
   * Returns whether the clustering sort is enabled.
   */
  public static boolean sortClusteringEnabled(Configuration conf) {
    return !StringUtils.isNullOrEmpty(conf.get(FlinkOptions.CLUSTERING_SORT_COLUMNS));
  }

  /**
   * Returns whether the operation is INSERT OVERWRITE (table or partition).
   */
  public static boolean isInsertOverwrite(Configuration conf) {
    return conf.get(FlinkOptions.OPERATION).equalsIgnoreCase(WriteOperationType.INSERT_OVERWRITE_TABLE.value())
        || conf.get(FlinkOptions.OPERATION).equalsIgnoreCase(WriteOperationType.INSERT_OVERWRITE.value());
  }

  /**
   * Returns whether the operation is INSERT OVERWRITE dynamic partition.
   */
  public static boolean overwriteDynamicPartition(Configuration conf) {
    return conf.get(FlinkOptions.OPERATION).equalsIgnoreCase(WriteOperationType.INSERT_OVERWRITE.value())
        || conf.get(FlinkOptions.WRITE_PARTITION_OVERWRITE_MODE).equalsIgnoreCase(PartitionOverwriteMode.DYNAMIC.name());
  }

  /**
   * Returns whether the read start commit is specific commit timestamp.
   */
  public static boolean isSpecificStartCommit(Configuration conf) {
    return conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent()
        && !conf.get(FlinkOptions.READ_START_COMMIT).equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST);
  }

  /**
   * Returns true if there are no explicit start and end commits.
   */
  public static boolean hasNoSpecificReadCommits(Configuration conf) {
    return !conf.contains(FlinkOptions.READ_START_COMMIT) && !conf.contains(FlinkOptions.READ_END_COMMIT);
  }

  /**
   * Returns the read commits limit or -1 if not specified.
   */
  public static int getReadCommitsLimit(Configuration conf) {
    return conf.getOptional(FlinkOptions.READ_COMMITS_LIMIT).orElse(-1);
  }

  /**
   * Returns the supplemental logging mode.
   */
  public static HoodieCDCSupplementalLoggingMode getCDCSupplementalLoggingMode(Configuration conf) {
    String mode = conf.get(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE).toUpperCase();
    return HoodieCDCSupplementalLoggingMode.valueOf(mode);
  }

  /**
   * Returns whether comprehensive schema evolution enabled.
   */
  public static boolean isSchemaEvolutionEnabled(Configuration conf) {
    return Boolean.parseBoolean(conf.getString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue().toString()));
  }

  /**
   * Returns whether the query is incremental.
   */
  public static boolean isIncrementalQuery(Configuration conf) {
    return conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent() || conf.getOptional(FlinkOptions.READ_END_COMMIT).isPresent();
  }

  /**
   * Returns whether consistent value will be generated for a logical timestamp type column.
   */
  public static boolean isConsistentLogicalTimestampEnabled(Configuration conf) {
    return Boolean.parseBoolean(conf.getString(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
  }

  /**
   * Returns whether the writer txn should be guarded by lock.
   */
  public static boolean isLockRequired(Configuration conf) {
    return conf.get(FlinkOptions.METADATA_ENABLED) || isMultiWriter(conf);
  }

  /**
   * Returns whether multi-writer is enabled.
   */
  public static boolean isMultiWriter(Configuration conf) {
    return WriteConcurrencyMode.supportsMultiWriter(conf.getString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue()));
  }

  /**
   * Returns whether to read the instants using completion time.
   *
   * <p>A Hudi instant contains both the txn start time and completion time, for incremental subscription
   * of the source reader, using completion time to filter the candidate instants can avoid data loss
   * in scenarios like multiple writers.
   */
  public static boolean isReadByTxnCompletionTime(Configuration conf) {
    HollowCommitHandling handlingMode = HollowCommitHandling.valueOf(conf
        .getString(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.defaultValue()));
    return handlingMode == HollowCommitHandling.USE_TRANSITION_TIME;
  }

  /**
   * Returns whether to read the data changes only from changelog files. When CDC is enabled,
   * i) for COW table, the changelog is generated on each file update;
   * ii) for MOR table, the changelog is generated on compaction.
   *
   * <p>By default, always read from the changelog file,
   * once it is disabled, the reader would infer the changes based on the file slice dependencies.
   */
  public static boolean readCDCFromChangelog(Configuration conf) {
    return conf.get(FlinkOptions.READ_CDC_FROM_CHANGELOG);
  }

  /**
   * Returns whether to populate meta fields or not
   */
  public static boolean isPopulateMetaFields(Configuration conf) {
    return Boolean.parseBoolean(
        conf.getString(
            HoodieTableConfig.POPULATE_META_FIELDS.key(),
            HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString()));
  }

  /**
   * Returns the index type.
   */
  public static HoodieIndex.IndexType getIndexType(Configuration conf) {
    return HoodieIndex.IndexType.valueOf(conf.get(FlinkOptions.INDEX_TYPE).toUpperCase());
  }

  /**
   * Returns the index key field.
   */
  public static String getIndexKeyField(Configuration conf) {
    return conf.getString(FlinkOptions.INDEX_KEY_FIELD.key(), conf.get(FlinkOptions.RECORD_KEY_FIELD));
  }

  /**
   * Returns the index key field values.
   */
  public static String[] getIndexKeys(Configuration conf) {
    return getIndexKeyField(conf).split(",");
  }

  /**
   * Returns the conflict resolution strategy.
   */
  public static ConflictResolutionStrategy getConflictResolutionStrategy(Configuration conf) {
    return isBucketIndexType(conf)
        ? new BucketIndexConcurrentFileWritesConflictResolutionStrategy()
        : new SimpleConcurrentFileWritesConflictResolutionStrategy();
  }

  /**
   * Returns the IO related options as a map.
   */
  public static Map<String, String> getIOOptions(Configuration conf) {
    Map<String, String> confMap = new HashMap<>();
    // file writer options
    confMap.put(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HoodieFlinkIOFactory.class.getName());
    confMap.put(HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), conf.get(FlinkOptions.WRITE_UTC_TIMEZONE).toString());

    // file reader options
    confMap.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(),
        conf.getString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue().toString()));
    confMap.put(FlinkOptions.READ_UTC_TIMEZONE.key(), conf.get(FlinkOptions.READ_UTC_TIMEZONE).toString());

    confMap.put(FlinkOptions.PARTITION_PATH_FIELD.key(), conf.get(FlinkOptions.PARTITION_PATH_FIELD));
    confMap.put(FlinkOptions.PARTITION_DEFAULT_NAME.key(), conf.get(FlinkOptions.PARTITION_DEFAULT_NAME));
    confMap.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING).toString());
    return confMap;
  }

  /**
   * Returns whether to commit even when current batch has no data, for flink defaults false
   */
  public static boolean allowCommitOnEmptyBatch(Configuration conf) {
    return Boolean.parseBoolean(conf.getString(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), HoodieWriteConfig.ALLOW_EMPTY_COMMIT.defaultValue().toString()));
  }

  /**
   * Returns whether this is non-blocking concurrency control.
   */
  public static boolean isNonBlockingConcurrencyControl(Configuration config) {
    return WriteConcurrencyMode.isNonBlockingConcurrencyControl(config.getString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue()));
  }

  /**
   * Returns whether Cleaner's failed writes policy is set to lazy
   */
  public static boolean isLazyFailedWritesCleanPolicy(Configuration conf) {
    // get all keys with alternatives as strings from Hudi's ConfigProperty
    List<String> allKeys = new ArrayList<>();
    allKeys.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key());
    allKeys.addAll(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.getAlternatives());
    // and check them in Flink's Configuration for all key variations support
    for (String key : allKeys) {
      if (conf.containsKey(key)) {
        return conf.getString(key, "").equalsIgnoreCase(HoodieFailedWritesCleaningPolicy.LAZY.name());
      }
    }
    return HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.defaultValue().equalsIgnoreCase(HoodieFailedWritesCleaningPolicy.LAZY.name());
  }

  /**
   * Returns whether the writers should use blocking instant time generation.
   */
  public static boolean isBlockingInstantGeneration(Configuration conf) {
    return isCowTable(conf) && isUpsertOperation(conf);
  }

  /**
   * Returns the customized insert partitioner instance.
   */
  public static Option<Partitioner> getInsertPartitioner(Configuration conf) {
    String insertPartitionerClass = conf.get(FlinkOptions.INSERT_PARTITIONER_CLASS_NAME);
    try {
      return StringUtils.isNullOrEmpty(insertPartitionerClass)
          ? Option.empty()
          : Option.of((Partitioner) ReflectionUtils.loadClass(insertPartitionerClass, conf));
    } catch (Throwable e) {
      throw new HoodieException("Could not create custom insert partitioner " + insertPartitionerClass, e);
    }
  }

  /**
   * Returns whether complex keygen encodes single record key with field name.
   */
  public static boolean useComplexKeygenOldEncoding(Configuration conf) {
    return Boolean.parseBoolean(conf.getString(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(),
        HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.defaultValue().toString()));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Returns all the config options with the given class {@code clazz}.
   */
  public static List<ConfigOption<?>> allOptions(Class<?> clazz) {
    Field[] declaredFields = clazz.getDeclaredFields();
    List<ConfigOption<?>> options = new ArrayList<>();
    for (Field field : declaredFields) {
      if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
          && field.getType().equals(ConfigOption.class)) {
        try {
          options.add((ConfigOption<?>) field.get(ConfigOption.class));
        } catch (IllegalAccessException e) {
          throw new HoodieException("Error while fetching static config option", e);
        }
      }
    }
    return options;
  }

  /**
   * Whether the reader only consumes new commit instants.
   */
  public static boolean isOnlyConsumingNewCommits(Configuration conf) {
    return isMorTable(conf) && conf.get(FlinkOptions.READ_STREAMING_SKIP_COMPACT) // this is only true for flink.
        || isAppendMode(conf) && conf.get(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING);
  }
}
