/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * Base class for all of the custom Hudi's RDD implementations
 *
 * NOTE: It enforces, for ex, that all of the RDDs implement [[compute]] method returning
 *       [[InternalRow]] to avoid superfluous ser/de
 */
abstract class HoodieBaseRDD(@transient sc: SparkContext)
  extends RDD[InternalRow](sc, Nil) {

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow]

}
