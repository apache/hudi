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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tool helping to resolve the flink options {@link FlinkOptions}.
 */
public class OptionsResolver {
  /**
   * Returns whether insert clustering is allowed with given configuration {@code conf}.
   */
  public static boolean insertClustering(Configuration conf) {
    return isCowTable(conf) && isInsertOperation(conf) && conf.getBoolean(FlinkOptions.INSERT_CLUSTER);
  }

  /**
   * Returns whether the insert is clustering disabled with given configuration {@code conf}.
   */
  public static boolean isAppendMode(Configuration conf) {
    // 1. inline clustering is supported for COW table;
    // 2. async clustering is supported for both COW and MOR table
    return isInsertOperation(conf) && ((isCowTable(conf) && !conf.getBoolean(FlinkOptions.INSERT_CLUSTER)) || isMorTable(conf));
  }

  /**
   * Returns whether the table operation is 'insert'.
   */
  public static boolean isInsertOperation(Configuration conf) {
    WriteOperationType operationType = WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
    return operationType == WriteOperationType.INSERT;
  }

  /**
   * Returns whether it is a MERGE_ON_READ table.
   */
  public static boolean isMorTable(Configuration conf) {
    return conf.getString(FlinkOptions.TABLE_TYPE)
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
    return conf.getString(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
  }

  /**
   * Returns whether the payload clazz is {@link DefaultHoodieRecordPayload}.
   */
  public static boolean isDefaultHoodieRecordPayloadClazz(Configuration conf) {
    return conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME).contains(DefaultHoodieRecordPayload.class.getSimpleName());
  }

  /**
   * Returns the preCombine field
   * or null if the value is set as {@link FlinkOptions#NO_PRE_COMBINE}.
   */
  public static String getPreCombineField(Configuration conf) {
    final String preCombineField = conf.getString(FlinkOptions.PRECOMBINE_FIELD);
    return preCombineField.equals(FlinkOptions.NO_PRE_COMBINE) ? null : preCombineField;
  }

  /**
   * Returns whether the compaction strategy is based on elapsed delta time.
   */
  public static boolean isDeltaTimeCompaction(Configuration conf) {
    final String strategy = conf.getString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toLowerCase(Locale.ROOT);
    return FlinkOptions.TIME_ELAPSED.equals(strategy) || FlinkOptions.NUM_OR_TIME.equals(strategy);
  }

  /**
   * Returns whether the table is partitioned.
   */
  public static boolean isPartitionedTable(Configuration conf) {
    return FilePathUtils.extractPartitionKeys(conf).length > 0;
  }

  public static boolean isBucketIndexType(Configuration conf) {
    return conf.getString(FlinkOptions.INDEX_TYPE).equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name());
  }

  /**
   * Returns whether to read the instants using completion time.
   *
   * <p>A Hudi instant contains both the txn start time and completion time, for incremental subscription
   * of the source reader, using completion time to filter the candidate instants can avoid data loss
   * in scenarios like multiple writers.
   */
  public static boolean isReadByTxnCompletionTime(Configuration conf) {
    return conf.getBoolean(HoodieCommonConfig.READ_BY_STATE_TRANSITION_TIME.key(), HoodieCommonConfig.READ_BY_STATE_TRANSITION_TIME.defaultValue());
  }

  /**
   * Returns whether the source should emit changelog.
   *
   * @return true if the source is read as streaming with changelog mode enabled
   */
  public static boolean emitChangelog(Configuration conf) {
    return conf.getBoolean(FlinkOptions.READ_AS_STREAMING) && conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED)
        || conf.getBoolean(FlinkOptions.READ_AS_STREAMING) && conf.getBoolean(FlinkOptions.CDC_ENABLED)
        || isIncrementalQuery(conf) && conf.getBoolean(FlinkOptions.CDC_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the async compaction.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsAsyncCompaction(Configuration conf) {
    return OptionsResolver.isMorTable(conf)
        && conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the compaction plan.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleCompaction(Configuration conf) {
    return OptionsResolver.isMorTable(conf)
        && conf.getBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the async clustering.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsAsyncClustering(Configuration conf) {
    return isInsertOperation(conf) && conf.getBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED);
  }

  /**
   * Returns whether there is need to schedule the clustering plan.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleClustering(Configuration conf) {
    return isInsertOperation(conf) && conf.getBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED);
  }

  /**
   * Returns whether the clustering sort is enabled.
   */
  public static boolean sortClusteringEnabled(Configuration conf) {
    return !StringUtils.isNullOrEmpty(conf.getString(FlinkOptions.CLUSTERING_SORT_COLUMNS));
  }

  /**
   * Returns whether the operation is INSERT OVERWRITE (table or partition).
   */
  public static boolean isInsertOverwrite(Configuration conf) {
    return conf.getString(FlinkOptions.OPERATION).equals(WriteOperationType.INSERT_OVERWRITE_TABLE.value())
        || conf.getString(FlinkOptions.OPERATION).equals(WriteOperationType.INSERT_OVERWRITE.value());
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
   * Returns the supplemental logging mode.
   */
  public static HoodieCDCSupplementalLoggingMode getCDCSupplementalLoggingMode(Configuration conf) {
    String mode = conf.getString(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE);
    return HoodieCDCSupplementalLoggingMode.valueOf(mode);
  }

  /**
   * Returns whether comprehensive schema evolution enabled.
   */
  public static boolean isSchemaEvolutionEnabled(Configuration conf) {
    return conf.getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue());
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
    return conf.getBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
  }

  /**
   * Returns whether to commit even when current batch has no data, for flink defaults false
   */
  public static boolean allowCommitOnEmptyBatch(Configuration conf) {
    return conf.getBoolean(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), false);
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
}
