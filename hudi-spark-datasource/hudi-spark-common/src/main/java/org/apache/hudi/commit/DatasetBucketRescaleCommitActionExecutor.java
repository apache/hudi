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
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.index.bucket.PartitionBucketIndexCalculator;
import org.apache.hudi.index.bucket.PartitionBucketIndexUtils;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DatasetBucketRescaleCommitActionExecutor extends DatasetBulkInsertOverwriteCommitActionExecutor {

  private final PartitionBucketIndexCalculator calc;

  public DatasetBucketRescaleCommitActionExecutor(HoodieWriteConfig config,
                                                  SparkRDDWriteClient writeClient,
                                                  String instantTime) {
    super(config, writeClient, instantTime);
    String expression = config.getBucketIndexPartitionExpression();
    String instant = config.getHashingConfigInstantToLoad();
    String rule = config.getBucketIndexPartitionRuleType();
    int bucketNumber = config.getBucketIndexNumBuckets();
    PartitionBucketIndexHashingConfig hashingConfig = new PartitionBucketIndexHashingConfig(expression,
        bucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, instant);
    this.calc = PartitionBucketIndexCalculator.getInstance(instantTime, hashingConfig);
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
        writeConfig.getBucketIndexNumBuckets(), calc);
  }

  /**
   * create new hashing_config during afterExecute and before commit finished
   * @param result
   */
  @Override
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    super.afterExecute(result);

    PartitionBucketIndexHashingConfig hashingConfig = calc.getHashingConfig();
    boolean res = PartitionBucketIndexUtils.saveHashingConfig(hashingConfig, table.getMetaClient());
    ValidationUtils.checkArgument(res);
  }
}
