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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.conf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

case class BatchedBlobStrategy(sparkSession: SparkSession) extends SparkStrategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case read @ BatchedBlobRead(child, _, _) =>
      // TODO find proper way to access these configs
      val maxGapBytes =
        conf.getConfString("hoodie.blob.batching.max.gap.bytes", "4096").toInt
      val lookaheadSize =
        conf.getConfString("hoodie.blob.batching.lookahead.size", "50").toInt

      val storageConf = new HadoopStorageConfiguration(sparkSession.sparkContext.hadoopConfiguration)
      BatchedBlobReadExec(
        planLater(child),
        maxGapBytes,
        storageConf,
        lookaheadSize,
        read
      ) :: Nil

    case _ => Nil
  }
}
