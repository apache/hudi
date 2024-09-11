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

package org.apache.hudi.util

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.parquet.TestSparkParquetReaderFormat
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.function.Predicate

object JavaConversions extends SparkAdapterSupport {
  def getPredicate[T](function1: (T) => Boolean): Predicate[T] = {
    new Predicate[T] {
      override def test(t: T): Boolean = function1.apply(t)
    }
  }

  def getFunction[T, R](function: Function[T, R]): java.util.function.Function[T, R] = {
    new java.util.function.Function[T, R] {
      override def apply(t: T): R = {
        function.apply(t)
      }
    }
  }

  /**
   * Read parquet files using [[TestSparkParquetReaderFormat]]
   *
   * @param sparkSession the spark session
   * @param paths comma seperated list of parquet files or directories containing parquet files
   * @return dataframe containing the data from the input paths
   */
  def createTestDataFrame(sparkSession: SparkSession, paths: String): DataFrame = {
    sparkSession.sqlContext.baseRelationToDataFrame(DataSource.apply(
      sparkSession = sparkSession,
      className = "org.apache.spark.sql.execution.datasources.parquet.TestSparkParquetReaderFormat",
      paths =  paths.split(",").toSeq
    ).resolveRelation())
  }
}
