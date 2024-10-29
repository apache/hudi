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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.HoodieSparkExtensibleBucketIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

// TODO: complete the implementation for extensible bucket
public class RDDExtensibleBucketBulkInsertPartitioner<T> extends RDDBucketIndexPartitioner<T> {
  public RDDExtensibleBucketBulkInsertPartitioner(HoodieTable table,
                                                  Map<String, String> strategyParams,
                                                  boolean preserveHoodieMetadata) {
    super(table,
        strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), null),
        preserveHoodieMetadata);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "Extensible bucket index doesn't support CoW table");
  }

  public RDDExtensibleBucketBulkInsertPartitioner(HoodieTable table) {
    this(table, Collections.emptyMap(), false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSparkExtensibleBucketIndex,
        "RDDExtensibleBucketPartitioner can only be used together with extensible bucket index");
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputPartitions) {
    return null;
  }
}
