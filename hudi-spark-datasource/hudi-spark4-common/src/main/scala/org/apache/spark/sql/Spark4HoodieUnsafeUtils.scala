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

import org.apache.hudi.{HoodieUnsafeRDD, SparkAdapterSupport}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair

/**
 * Suite of utilities helping in handling instances of [[HoodieUnsafeRDD]]
 */
object Spark4HoodieUnsafeUtils extends HoodieUnsafeUtils {

  /**
   * Fetches expected output [[Partitioning]] of the provided [[DataFrame]]
   *
   * NOTE: Invoking [[QueryExecution#executedPlan]] wouldn't actually execute the query (ie start pumping the data)
   *       but instead will just execute Spark resolution, optimization and actual execution planning stages
   *       returning instance of [[SparkPlan]] ready for execution
   */
  def getNumPartitions(df: DataFrame): Int = {
    // NOTE: In general we'd rely on [[outputPartitioning]] of the executable [[SparkPlan]] to determine
    //       number of partitions plan is going to be executed with.
    //       However in case of [[LogicalRDD]] plan's output-partitioning will be stubbed as [[UnknownPartitioning]]
    //       and therefore we will be falling back to determine number of partitions by looking at the RDD itself
    df.queryExecution.logical match {
      case LogicalRDD(_, rdd, outputPartitioning, _, _, _) =>
        outputPartitioning match {
          case _: UnknownPartitioning => rdd.getNumPartitions
          case _ => outputPartitioning.numPartitions
        }

      case _ => df.queryExecution.executedPlan.outputPartitioning.numPartitions
    }
  }

  /**
   * Creates [[DataFrame]] from provided [[plan]]
   *
   * @param spark spark's session
   * @param plan given plan to wrap into [[DataFrame]]
   */
  def createDataFrameFrom(spark: SparkSession, plan: LogicalPlan): DataFrame =
    org.apache.spark.sql.classic.Dataset.ofRows(spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession], plan)

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
  def createDataFrameFromRows(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame =
    org.apache.spark.sql.classic.Dataset.ofRows(spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession], LocalRelation.fromExternalRows(
      SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema), rows))

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
  def createDataFrameFromInternalRows(spark: SparkSession, rows: Seq[InternalRow], schema: StructType): DataFrame =
    org.apache.spark.sql.classic.Dataset.ofRows(spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession], LocalRelation(SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema), rows))


  /**
   * Creates [[DataFrame]] from the [[RDD]] of [[Row]]s with provided [[schema]]
   *
   * @param spark spark's session
   * @param rdd RDD w/ [[Row]]s to base [[DataFrame]] on
   * @param schema target [[DataFrame]]'s schema
   */
  def createDataFrameFromRDD(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType): DataFrame =
    spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession].internalCreateDataFrame(rdd, schema)

  /**
   * Canonical implementation of the [[RDD#collect]] for [[HoodieUnsafeRDD]], returning a properly
   * copied [[Array]] of [[InternalRow]]s
   */
  def collect(rdd: HoodieUnsafeRDD): Array[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      // NOTE: We're leveraging [[MutablePair]] here to avoid unnecessary allocations, since
      //       a) iteration is performed lazily and b) iteration is single-threaded (w/in partition)
      val pair = new MutablePair[InternalRow, Null]()
      iter.map(row => pair.update(row.copy(), null))
    }
      .map(p => p._1)
      .collect()
  }
}
