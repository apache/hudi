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

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.DataSourceInternalWriterHelper;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetBulkInsertCommitActionExecutor extends BaseDatasetBulkInsertCommitActionExecutor {

  public DatasetBulkInsertCommitActionExecutor(HoodieWriteConfig config,
                                               SparkRDDWriteClient writeClient) {
    super(config, writeClient);
  }

  @Override
  protected void preExecute() {
    instantTime = writeClient.startCommit();
    table = writeClient.initTable(getWriteOperationType(), Option.ofNullable(instantTime));
  }

  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    Map<String, String> opts = writeConfig.getProps().entrySet().stream().collect(Collectors.toMap(
        e -> String.valueOf(e.getKey()),
        e -> String.valueOf(e.getValue())));
    Map<String, String> optsOverrides = Collections.singletonMap(
        HoodieInternalConfig.BULKINSERT_ARE_PARTITIONER_RECORDS_SORTED, String.valueOf(arePartitionRecordsSorted));

    String targetFormat;
    Map<String, String> customOpts = new HashMap<>(1);
    if (HoodieSparkUtils.isSpark3()) {
      targetFormat = "org.apache.hudi.spark.internal";
      customOpts.put(HoodieInternalConfig.BULKINSERT_INPUT_DATA_SCHEMA_DDL.key(), records.schema().json());
    } else {
      throw new HoodieException("Bulk insert using row writer is not supported with current Spark version."
          + " To use row writer please switch to spark 3");
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
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    // no op
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.BULK_INSERT;
  }
}
