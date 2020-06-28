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

package org.apache.hudi.table.action.commit.dataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.hudi.client.EncodableWriteStatus;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.utils.SparkConfigUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertDatasetPartitioner;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieDatasetWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class BulkInsertDatasetCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseActionExecutor<HoodieDatasetWriteMetadata> {

  private static final Logger LOG = LogManager
      .getLogger(BulkInsertDatasetCommitActionExecutor.class);

  private final Dataset<Row> rowDataset;
  private final Option<UserDefinedBulkInsertDatasetPartitioner> bulkInsertPartitioner;

  private final WriteOperationType operationType;
  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  public BulkInsertDatasetCommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, Dataset<Row> rowDataset,
      Option<UserDefinedBulkInsertDatasetPartitioner> bulkInsertPartitioner) {
    super(jsc, config, table, instantTime);
    this.operationType = WriteOperationType.BULK_INSERT_DATASET;
    this.rowDataset = rowDataset;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  @Override
  public HoodieDatasetWriteMetadata execute() {
    try {
      return BulkInsertDatasetHelper
          .bulkInsertDataset(new SQLContext(jsc.sc()),
              rowDataset, instantTime, (HoodieTable<T>) table, config,
              this, true, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }

  protected void updateIndexAndCommitIfNeeded(Dataset<EncodableWriteStatus> encWriteStatusDataset,
      HoodieDatasetWriteMetadata result) {
    // cache encWriteStatusDataset before updating index, so that all actions before this are not triggered again for future
    // Dataset actions that are performed after updating the index.
    encWriteStatusDataset = encWriteStatusDataset
        .persist(SparkConfigUtils.getWriteStatusStorageLevel(config.getProps()));
    Instant indexStartTime = Instant.now();
    // Update the index back
    Dataset<EncodableWriteStatus> statuses = ((HoodieTable<T>) table).getIndex()
        .updateLocation(encWriteStatusDataset, jsc,
            (HoodieTable<T>) table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setEncodableWriteStatuses(statuses);
    // TODO fix
    commitOnAutoCommit(result);
  }

  protected void commitOnAutoCommit(HoodieDatasetWriteMetadata result) {
    if (config.shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      commit(Option.empty(), result);
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  private void commit(Option<Map<String, String>> extraMetadata,
      HoodieDatasetWriteMetadata result) {
    String actionType = table.getMetaClient().getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, jsc);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    result.setCommitted(true);
    List<HoodieWriteStat> stats = result.getEncodableWriteStatuses().toJavaRDD()
        .map(EncodableWriteStatus::getStat).collect();
    result.setWriteStats(stats);

    updateMetadataAndRollingStats(metadata, stats);

    // Finalize write
    finalizeWrite(instantTime, stats, result);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(operationType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException(
          "Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    result.setCommitMetadata(Option.of(metadata));
  }

  /**
   * Finalize Write operation.
   *
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats,
      HoodieDatasetWriteMetadata result) {
    try {
      Instant start = Instant.now();
      table.finalizeWrite(jsc, instantTime, stats);
      result.setFinalizeDuration(Duration.between(start, Instant.now()));
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException(
          "Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  private void updateMetadataAndRollingStats(HoodieCommitMetadata metadata,
      List<HoodieWriteStat> writeStats) {
    // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
    // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

    // Need to do this on every commit (delta or commit) to support COW and MOR.
    for (HoodieWriteStat stat : writeStats) {
      String partitionPath = stat.getPartitionPath();
      // TODO: why is stat.getPartitionPath() null at times here.
      metadata.addWriteStat(partitionPath, stat);
    }
  }
}
