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
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieUnsafeUtils$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession$;

public abstract class SparkBulkInsertPartitionerBase<T> implements BulkInsertPartitioner<T> {

  protected static Dataset<Row> tryCoalesce(Dataset<Row> dataset, int targetPartitionNumHint) {
    // NOTE: In case incoming [[Dataset]]'s partition count matches the target one,
    //       we short-circuit coalescing altogether (since this isn't done by Spark itself)
    if (targetPartitionNumHint > 0 && targetPartitionNumHint != HoodieUnsafeUtils$.MODULE$.getNumPartitions(dataset)) {
      return dataset.coalesce(targetPartitionNumHint);
    }

    return dataset;
  }

  protected static <T> JavaRDD<HoodieRecord<T>> tryCoalesce(JavaRDD<HoodieRecord<T>> records,
                                                            int targetPartitionNumHint) {
    // NOTE: In case incoming [[RDD]]'s partition count matches the target one,
    //       we short-circuit coalescing altogether (since this isn't done by Spark itself)
    if (targetPartitionNumHint > 0 && targetPartitionNumHint != records.getNumPartitions()) {
      return records.coalesce(targetPartitionNumHint);
    }

    return records;
  }

  // TODO java-doc
  protected static int handleTargetPartitionNumHint(int targetPartitionNum) {
    return targetPartitionNum > 0 ? targetPartitionNum : getDefaultParallelism();
  }

  protected static int getDefaultParallelism() {
    return SparkSession$.MODULE$.active()
        .sparkContext()
        .defaultParallelism();
  }
}
