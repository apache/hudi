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

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class LSMDatasetUpsertCommitActionExecutor extends LSMDatasetBulkInsertCommitActionExecutor {

  public LSMDatasetUpsertCommitActionExecutor(HoodieWriteConfig config,
                                              SparkRDDWriteClient writeClient,
                                              String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  public HoodieWriteResult execute(Dataset<Row> records, boolean isTablePartitioned) {
    ValidationUtils.checkArgument(writeConfig.getIndexType() == HoodieIndex.IndexType.BUCKET);
    Dataset<Row> hoodieDF;
    // do repartition and sort by
    hoodieDF = HoodieDatasetBulkInsertHelper.prepareForLsm(records, writeConfig);
    table = writeClient.initTable(getWriteOperationType(), Option.ofNullable(instantTime));
    preExecute();
    // always do sort(sort by instead of order by)
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = buildHoodieWriteMetadata(doExecute(hoodieDF, true));
    afterExecute(result);

    return new HoodieWriteResult(result.getWriteStatuses(), result.getPartitionToReplaceFileIds());
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.UPSERT;
  }
}
