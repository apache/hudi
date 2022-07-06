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

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.configuration.Configuration;

import java.util.Locale;

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
    return isCowTable(conf) && isInsertOperation(conf) && !conf.getBoolean(FlinkOptions.INSERT_CLUSTER)
        || needsScheduleClustering(conf);
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
    return conf.getString(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name());
  }

  /**
   * Returns whether the source should emit changelog.
   *
   * @return true if the source is read as streaming with changelog mode enabled
   */
  public static boolean emitChangelog(Configuration conf) {
    return conf.getBoolean(FlinkOptions.READ_AS_STREAMING)
        && conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED);
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
}
