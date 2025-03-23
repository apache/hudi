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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.index.bucket.PartitionBucketIndexUtils;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DatasetBucketRescaleCommitActionExecutor extends DatasetBulkInsertOverwriteCommitActionExecutor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(DatasetBucketRescaleCommitActionExecutor.class);
  private final PartitionBucketIndexHashingConfig hashingConfig;

  public DatasetBucketRescaleCommitActionExecutor(HoodieWriteConfig config,
                                                  SparkRDDWriteClient writeClient,
                                                  String instantTime) {
    super(config, writeClient, instantTime);
    String expression = config.getBucketIndexPartitionExpression();
    String instant = config.getHashingConfigInstantToLoad();
    String rule = config.getBucketIndexPartitionRuleType();
    int bucketNumber = config.getBucketIndexNumBuckets();
    this.hashingConfig = new PartitionBucketIndexHashingConfig(expression,
        bucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, instant);
  }

  /**
   * Create BulkInsertPartitioner with prepared PartitionBucketIndexCalculator
   * @param populateMetaFields
   * @param isTablePartitioned
   * @return BulkInsertPartitioner
   */
  @Override
  protected BulkInsertPartitioner<Dataset<Row>> getPartitioner(boolean populateMetaFields, boolean isTablePartitioned) {
    return new BucketIndexBulkInsertPartitionerWithRows(writeConfig.getBucketIndexHashFieldWithDefault(),
        writeConfig.getBucketIndexNumBuckets(), hashingConfig);
  }

  /**
   * create new hashing_config during afterExecute and before commit finished
   * @param result
   */
  @Override
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    super.afterExecute(result);
    boolean res = PartitionBucketIndexUtils.saveHashingConfig(hashingConfig, table.getMetaClient());
    ValidationUtils.checkArgument(res);
    LOG.info("Finish to save hashing config " + hashingConfig);
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses) {
    return HoodieJavaPairRDD.getJavaPairRDD(writeStatuses.map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
        Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
  }
}
