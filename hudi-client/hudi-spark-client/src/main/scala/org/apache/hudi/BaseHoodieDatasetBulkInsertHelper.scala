/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.table.action.commit.ParallelismHelper
import org.apache.hudi.util.JFunction.toJavaSerializableFunctionUnchecked

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HoodieUnsafeUtils.getNumPartitions
import org.apache.spark.sql.DataFrame

object BaseHoodieDatasetBulkInsertHelper
  extends ParallelismHelper[DataFrame](toJavaSerializableFunctionUnchecked(df => getNumPartitions(df)))
    with HoodieDatasetBulkInsertHelper
    with Logging
    with Serializable {

  override protected def deduceShuffleParallelism(input: DataFrame, configuredParallelism: Int): Int = {
    val deduceParallelism = super.deduceShuffleParallelism(input, configuredParallelism)
    // NOTE: In case parallelism deduction failed to accurately deduce parallelism level of the
    //       incoming dataset we fallback to default parallelism level set for this Spark session
    if (deduceParallelism > 0) {
      deduceParallelism
    } else {
      input.sparkSession.sparkContext.defaultParallelism
    }
  }
}
