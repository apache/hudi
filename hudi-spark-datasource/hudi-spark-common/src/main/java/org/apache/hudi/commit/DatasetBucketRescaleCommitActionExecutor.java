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
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetBucketRescaleCommitActionExecutor extends DatasetBulkInsertCommitActionExecutor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(DatasetBucketRescaleCommitActionExecutor.class);
  private final PartitionBucketIndexHashingConfig hashingConfig;

  public DatasetBucketRescaleCommitActionExecutor(HoodieWriteConfig config,
                                                  SparkRDDWriteClient writeClient,
                                                  String instantTime) {
    super(config, writeClient, instantTime, WriteOperationType.BUCKET_RESCALE);
    String expression = config.getBucketIndexPartitionExpression();
    String rule = config.getBucketIndexPartitionRuleType();
    int bucketNumber = config.getBucketIndexNumBuckets();
    this.hashingConfig = new PartitionBucketIndexHashingConfig(expression,
        bucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, instantTime);
  }

  /**
   * Create BulkInsertPartitioner with prepared PartitionBucketIndexCalculator.
   */
  @Override
  protected BulkInsertPartitioner<Dataset<Row>> getPartitioner(boolean populateMetaFields, boolean isTablePartitioned) {
    return new BucketIndexBulkInsertPartitionerWithRows(writeConfig.getBucketIndexHashFieldWithDefault(), hashingConfig);
  }

  /**
   * create new hashing_config during afterExecute and before commit finished.
   */
  @Override
  protected void preExecute() {
    super.preExecute();
    boolean res = PartitionBucketIndexHashingConfig.saveHashingConfig(hashingConfig, table.getMetaClient());
    ValidationUtils.checkArgument(res);
    LOG.info("Finish to save hashing config " + hashingConfig);
  }
}
