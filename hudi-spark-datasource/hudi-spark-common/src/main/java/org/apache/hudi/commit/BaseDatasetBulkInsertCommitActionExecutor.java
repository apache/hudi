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

package org.apache.hudi.commit;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.ConsistentBucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

public abstract class BaseDatasetBulkInsertCommitActionExecutor implements Serializable {

  protected final transient HoodieWriteConfig writeConfig;
  protected final transient SparkRDDWriteClient writeClient;
  protected String instantTime;
  protected HoodieTable table;

  public BaseDatasetBulkInsertCommitActionExecutor(HoodieWriteConfig config,
                                                   SparkRDDWriteClient writeClient) {
    this.writeConfig = config;
    this.writeClient = writeClient;
  }

  protected void preExecute() {
    instantTime = writeClient.startCommit(getCommitActionType());
    table = writeClient.initTable(getWriteOperationType(), Option.ofNullable(instantTime));
    table.validateInsertSchema();
    writeClient.preWrite(instantTime, getWriteOperationType(), table.getMetaClient());
  }

  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED,
        getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false));
  }

  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    writeClient.postWrite(result, instantTime, table);
  }

  private HoodieWriteMetadata<JavaRDD<WriteStatus>> buildHoodieWriteMetadata(Option<HoodieData<WriteStatus>> writeStatuses) {
    table.getMetaClient().reloadActiveTimeline();
    return writeStatuses.map(statuses -> {
      // cache writeStatusRDD, so that all actions before this are not triggered again for future
      statuses.persist(writeConfig.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), writeClient.getEngineContext(), HoodieData.HoodieDataCacheKey.of(writeConfig.getBasePath(), instantTime));
      HoodieWriteMetadata<JavaRDD<WriteStatus>> hoodieWriteMetadata = new HoodieWriteMetadata<>();
      hoodieWriteMetadata.setWriteStatuses(HoodieJavaRDD.getJavaRDD(statuses));
      hoodieWriteMetadata.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(statuses));
      return hoodieWriteMetadata;
    }).orElseGet(HoodieWriteMetadata::new);
  }

  public final HoodieWriteResult execute(Dataset<Row> records, boolean isTablePartitioned) {
    if (writeConfig.getBoolean(DataSourceWriteOptions.INSERT_DROP_DUPS())) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet");
    }

    boolean populateMetaFields = writeConfig.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS);
    preExecute();

    BulkInsertPartitioner<Dataset<Row>> bulkInsertPartitionerRows = getPartitioner(populateMetaFields, isTablePartitioned);
    Dataset<Row> hoodieDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(records, writeConfig, bulkInsertPartitionerRows, instantTime);

    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = buildHoodieWriteMetadata(doExecute(hoodieDF, bulkInsertPartitionerRows.arePartitionRecordsSorted()));
    afterExecute(result);

    return new HoodieWriteResult(result.getWriteStatuses(), result.getPartitionToReplaceFileIds());
  }

  public abstract WriteOperationType getWriteOperationType();

  public String getCommitActionType() {
    return CommitUtils.getCommitActionType(getWriteOperationType(), writeClient.getConfig().getTableType());
  }

  protected BulkInsertPartitioner<Dataset<Row>> getPartitioner(boolean populateMetaFields, boolean isTablePartitioned) {
    if (populateMetaFields) {
      if (writeConfig.getIndexType() == HoodieIndex.IndexType.BUCKET) {
        if (writeConfig.getBucketIndexEngineType() == HoodieIndex.BucketIndexEngineType.SIMPLE) {
          return new BucketIndexBulkInsertPartitionerWithRows(writeConfig.getBucketIndexHashFieldWithDefault(), table.getConfig());
        } else {
          return new ConsistentBucketIndexBulkInsertPartitionerWithRows(table, Collections.emptyMap(), true);
        }
      } else {
        return DataSourceUtils
            .createUserDefinedBulkInsertPartitionerWithRows(writeConfig)
            .orElseGet(() -> BulkInsertInternalPartitionerWithRowsFactory.get(writeConfig, isTablePartitioned));
      }
    } else {
      // Sort modes are not yet supported when meta fields are disabled
      return new NonSortPartitionerWithRows();
    }
  }

  protected abstract Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses);

  public String getInstantTime() {
    return instantTime;
  }
}
