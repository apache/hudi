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

package org.apache.hudi.table.action.commit;

import java.time.Duration;
import java.time.Instant;
import org.apache.hudi.client.EncodableWriteStatus;
import org.apache.hudi.client.utils.SparkConfigUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class BulkInsertDatasetCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends CommitActionExecutor<T> {

  private final Dataset<Row> rowDataset;
  private final Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner;

  public BulkInsertDatasetCommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, Dataset<Row> rowDataset,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    super(jsc, config, table, instantTime, WriteOperationType.BULK_INSERT);
    this.rowDataset = rowDataset;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  @Override
  public HoodieWriteMetadata execute() {
    try {
      return BulkInsertDatasetHelper
          .bulkInsertDataset(rowDataset, instantTime, (HoodieTable<T>) table, config,
          this, true, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }

  protected void updateIndexAndCommitIfNeeded(Dataset<EncodableWriteStatus> encWriteStatusDataset, HoodieWriteMetadata result) {
    // cache encWriteStatusDataset before updating index, so that all actions before this are not triggered again for future
    // Dataset actions that are performed after updating the index.
    encWriteStatusDataset = encWriteStatusDataset.persist(SparkConfigUtils.getWriteStatusStorageLevel(config.getProps()));
    Instant indexStartTime = Instant.now();
    // Update the index back
    Dataset<EncodableWriteStatus> statuses = ((HoodieTable<T>) table).getIndex().updateLocation(encWriteStatusDataset, jsc,
        (HoodieTable<T>) table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setEncodableWriteStatuses(statuses);
    commitOnAutoCommit(result);
  }
}
