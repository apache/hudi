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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.JsonLinesErrorTableWriter;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

/**
 * The embedding job's properties and streamer configuration for tests ({@link UnstructuredDataSource} in mode embedding over an
 * S3 events table and {@link EmbeddingTableTransformer}), and readers of its table's commits.
 */
public final class EmbeddingStreamers {

  /** The suffix of the job's error table path, beside the embedding table. */
  public static final String ERRORS_SUFFIX = "_error_table";

  private EmbeddingStreamers() {
  }

  /**
   * The embedding job's properties for the embedding table {@code table} over the S3 events table {@code eventsTable}
   * ({@link UnstructuredDataSource} in mode embedding on cloud s3, objects read under {@code fsPrefix}, inline up to
   * {@code inlineMaxBytes}): a non-partitioned table, the streamer settings the job needs, the error table, then {@code overrides}.
   */
  public static TypedProperties jobProps(String table, String eventsTable, String fsPrefix, long inlineMaxBytes, String mode,
                                         TypedProperties overrides) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.partitionpath.field", "");
    props.setProperty("hoodie.datasource.write.keygenerator.class", NonpartitionedKeyGenerator.class.getName());
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), eventsTable);
    props.setProperty(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), fsPrefix);
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(inlineMaxBytes));
    props.putAll(overrides);
    TypedProperties job = commonProps(table, mode);
    job.putAll(props);
    return job;
  }

  /** The table, writer, streamer and error-table settings every embedding job here shares. */
  private static TypedProperties commonProps(String table, String mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), table);
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty("hoodie.datasource.write.recordkey.field", String.join(",", EmbeddingTableSchema.KEY_COLUMNS));
    props.setProperty("hoodie.datasource.write.hive_style_partitioning", "false");
    props.setProperty("hoodie.datasource.write.partitionpath.urlencode", "false");
    props.setProperty("hoodie.write.record.merge.mode", RecordMergeMode.COMMIT_TIME_ORDERING.name());
    props.setProperty("hoodie.datasource.write.reconcile.schema", "false");
    props.setProperty("hoodie.metadata.enable", "false");
    props.setProperty("hoodie.compact.inline", "false");
    props.setProperty("hoodie.clean.automatic", "false");
    props.setProperty("hoodie.upsert.shuffle.parallelism", "2");
    props.setProperty("hoodie.insert.shuffle.parallelism", "2");
    props.setProperty(EmbeddingTableConfig.MODE.key(), mode);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS.key(), JsonLinesErrorTableWriter.class.getName());
    props.setProperty(HoodieErrorTableConfig.ERROR_TARGET_TABLE.key(), "embedding_error_table");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key(), withoutTrailingSlash(table) + ERRORS_SUFFIX);
    return props;
  }

  /**
   * The streamer configuration of the embedding job on {@code table}: the unstructured data source, MERGE_ON_READ, UPSERT,
   * commit-time ordering.
   */
  public static HoodieStreamer.Config config(String table) {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = table;
    cfg.targetTableName = "embedding_table";
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.sourceClassName = UnstructuredDataSource.class.getName();
    cfg.transformerClassNames = Collections.singletonList(EmbeddingTableTransformer.class.getName());
    cfg.operation = WriteOperationType.UPSERT;
    cfg.sourceOrderingFields = null;
    cfg.recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    Triple<RecordMergeMode, String, String> merge = HoodieTableConfig.inferMergingConfigsForWrites(
        cfg.recordMergeMode, cfg.payloadClassName, cfg.recordMergeStrategyId, cfg.sourceOrderingFields,
        HoodieTableVersion.current());
    cfg.recordMergeMode = merge.getLeft();
    cfg.payloadClassName = merge.getMiddle();
    cfg.recordMergeStrategyId = merge.getRight();
    cfg.sourceLimit = Long.MAX_VALUE;
    cfg.forceDisableCompaction = true;
    return cfg;
  }

  // ------------------------------------------------------------------------------------------------------------ reading back

  /** The completed commits of {@code table} in completion order (none when there is no table). */
  public static List<HoodieInstant> commits(String table, StorageConfiguration<?> storageConf) {
    if (!EmbeddingTableChecks.tableExists(table, storageConf)) {
      return Collections.emptyList();
    }
    return metaClient(table, storageConf).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  public static HoodieTableMetaClient metaClient(String table, StorageConfiguration<?> storageConf) {
    return HoodieTableMetaClient.builder().setConf(storageConf.newInstance()).setBasePath(table).build();
  }

  public static StorageConfiguration<?> storageConf(SparkSession spark) {
    return HadoopFSUtils.getStorageConf(spark.sparkContext().hadoopConfiguration());
  }

  private static String withoutTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
