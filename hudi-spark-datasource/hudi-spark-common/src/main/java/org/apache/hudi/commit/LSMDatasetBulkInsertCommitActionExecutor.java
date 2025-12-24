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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.LSMBucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.internal.DataSourceInternalWriterHelper;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LSMDatasetBulkInsertCommitActionExecutor extends BaseDatasetBulkInsertCommitActionExecutor {

  public LSMDatasetBulkInsertCommitActionExecutor(HoodieWriteConfig config,
                                                  SparkRDDWriteClient writeClient,
                                                  String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  protected void preExecute() {
    // no op
  }

  // copy from DatasetBulkInsertCommitActionExecutor
  // TODO zhangyue143 还要支持Spark3.x
  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    Map<String, String> opts = writeConfig.getProps().entrySet().stream().collect(Collectors.toMap(
        e -> String.valueOf(e.getKey()),
        e -> String.valueOf(e.getValue())));
    Map<String, String> optsOverrides = new HashMap<>();
    optsOverrides.put(HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED, String.valueOf(arePartitionRecordsSorted));
    optsOverrides.put(HoodieTableConfig.HOODIE_LOG_FORMAT.key(), HoodieTableConfig.LSM_HOODIE_TABLE_LOG_FORMAT);

    String targetFormat;
    Map<String, String> customOpts = new HashMap<>(1);
    if (HoodieSparkUtils.isSpark2()) {
      targetFormat = "org.apache.hudi.internal";
    } else if (HoodieSparkUtils.isSpark3()) {
      targetFormat = "org.apache.hudi.spark3.internal";
      customOpts.put(HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key(), records.schema().json());
    } else {
      throw new HoodieException("Bulk insert using row writer is not supported with current Spark version."
          + " To use row writer please switch to spark 2 or spark 3");
    }

    records.write().format(targetFormat)
        .option(DataSourceInternalWriterHelper.INSTANT_TIME_OPT_KEY, instantTime)
        .options(opts)
        .options(customOpts)
        .options(optsOverrides)
        .mode(SaveMode.Append)
        .save();
    return Option.empty();
  }

  @Override
  public HoodieWriteResult execute(Dataset<Row> records, boolean isTablePartitioned) {
    table = writeClient.initTable(getWriteOperationType(), Option.ofNullable(instantTime));
    BucketIndexBulkInsertPartitionerWithRows bulkInsertPartitionerRows = getPartitioner(true, isTablePartitioned);
    boolean shouldDropPartitionColumns = writeConfig.getBoolean(DataSourceWriteOptions.DROP_PARTITION_COLUMNS());
    ValidationUtils.checkArgument(writeConfig.getIndexType() == HoodieIndex.IndexType.BUCKET);
    Dataset<Row> hoodieDF;
    // do repartition and sort by
    hoodieDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsertWithLSM(records, writeConfig, bulkInsertPartitionerRows, shouldDropPartitionColumns);
    preExecute();
    // always do sort(sort by instead of order by)
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = buildHoodieWriteMetadata(doExecute(hoodieDF, true));
    afterExecute(result);

    return new HoodieWriteResult(result.getWriteStatuses(), result.getPartitionToReplaceFileIds());
  }

  @Override
  protected BucketIndexBulkInsertPartitionerWithRows getPartitioner(boolean populateMetaFields, boolean isTablePartitioned) {

    return new LSMBucketIndexBulkInsertPartitionerWithRows(table.getConfig());
  }

  @Override
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    // no op
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.BULK_INSERT;
  }
}
