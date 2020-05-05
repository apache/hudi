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

package org.apache.hudi.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HoodieDatasetWriteClient<T extends HoodieRecordPayload> extends HoodieWriteClient<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieDatasetWriteClient.class);

  private static final long serialVersionUID = 1L;

  /**
   * Create a write client, with new hudi index.
   *
   * @param jsc Java Spark Context
   * @param clientConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieDatasetWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackPending) {
    super(jsc, clientConfig, rollbackPending, HoodieIndex.createIndex(clientConfig, jsc));
  }

  public HoodieDatasetWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackPending, HoodieIndex index) {
    super(jsc, clientConfig, rollbackPending, index, Option.empty());
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
   * loads into a Hoodie table for the very first time (e.g: converting an existing table to
   * Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
   * attempts to control the numbers of files with less memory compared to the {@link
   * HoodieWriteClient#insert(JavaRDD, String)}
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public Dataset<EncodableWriteStatus> bulkInsertDataset(Dataset<Row> records,
      final String instantTime) {
    return bulkInsertDataset(records, instantTime, Option.empty());
  }

  public Dataset<EncodableWriteStatus> bulkInsertDataset(Dataset<Row> records,
      final String instantTime, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.BULK_INSERT);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT);
    HoodieWriteMetadata result = table
        .bulkInsertDataset(jsc, instantTime, records, bulkInsertPartitioner);
    return postWriteDataset(result, instantTime, table);
  }

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations
   * including auto-commit.
   *
   * @param result Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  private Dataset<EncodableWriteStatus> postWriteDataset(HoodieWriteMetadata result,
      String instantTime, HoodieTable<T> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(),
          result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(),
          hoodieTable.getMetaClient().getCommitActionType());
    }
    return result.getEncodableWriteStatuses();
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses) {
    return commitDataset(instantTime, encWriteStatuses, Option.empty());
  }

  public boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses,
      Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commitDataset(instantTime, encWriteStatuses, extraMetadata, metaClient.getCommitActionType());
  }

  private boolean commitDataset(String instantTime, Dataset<EncodableWriteStatus> encWriteStatuses,
      Option<Map<String, String>> extraMetadata, String actionType) {
    LOG.info("Committing " + instantTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, jsc);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    List<HoodieWriteStat> stats = encWriteStatuses.toJavaRDD().map(EncodableWriteStatus::getStat)
        .collect();

    updateMetadataAndRollingStats(actionType, metadata, stats);

    // Finalize write
    finalizeWrite(table, instantTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(getOperationType());

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      postCommit(metadata, instantTime, extraMetadata);
      emitCommitMetrics(instantTime, metadata, actionType);
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException(
          "Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    return true;
  }

}
