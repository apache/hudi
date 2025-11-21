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

package org.apache.spark.sql

import org.apache.hudi.{HoodieUnsafeRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning}
import org.apache.spark.sql.types.StructType

/**
 * Suite of utilities helping in handling instances of [[HoodieUnsafeRDD]]
 */
trait HoodieUnsafeUtils {

  /**
   * Fetches expected output [[Partitioning]] of the provided [[DataFrame]]
   *
   * NOTE: Invoking [[QueryExecution#executedPlan]] wouldn't actually execute the query (ie start pumping the data)
   *       but instead will just execute Spark resolution, optimization and actual execution planning stages
   *       returning instance of [[SparkPlan]] ready for execution
   */
  def getNumPartitions(df: DataFrame): Int

  /**
   * Creates [[DataFrame]] from provided [[plan]]
   *
   * @param spark spark's session
   * @param plan given plan to wrap into [[DataFrame]]
   */
  def createDataFrameFrom(spark: SparkSession, plan: LogicalPlan): DataFrame

  /**
   * Creates [[DataFrame]] from the in-memory [[Seq]] of [[Row]]s with provided [[schema]]
   *
   * NOTE: [[DataFrame]] is based on [[LocalRelation]], entailing that most computations with it
   * will be executed by Spark locally
   *
   * @param spark  spark's session
   * @param rows   collection of rows to base [[DataFrame]] on
   * @param schema target [[DataFrame]]'s schema
   */
  def createDataFrameFromRows(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame

  /**
   * Creates [[DataFrame]] from the in-memory [[Seq]] of [[InternalRow]]s with provided [[schema]]
   *
   * NOTE: [[DataFrame]] is based on [[LocalRelation]], entailing that most computations with it
   *       will be executed by Spark locally
   *
   * @param spark spark's session
   * @param rows collection of rows to base [[DataFrame]] on
   * @param schema target [[DataFrame]]'s schema
   */
  def createDataFrameFromInternalRows(spark: SparkSession, rows: Seq[InternalRow], schema: StructType): DataFrame

  /**
   * Creates [[DataFrame]] from the [[RDD]] of [[Row]]s with provided [[schema]]
   *
   * @param spark spark's session
   * @param rdd RDD w/ [[Row]]s to base [[DataFrame]] on
   * @param schema target [[DataFrame]]'s schema
   */
  def createDataFrameFromRDD(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType): DataFrame

  /**
   * Canonical implementation of the [[RDD#collect]] for [[HoodieUnsafeRDD]], returning a properly
   * copied [[Array]] of [[InternalRow]]s
   */
  def collect(rdd: HoodieUnsafeRDD): Array[InternalRow]
}
