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

package org.apache.spark.sql.hudi.source

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{Batch, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

case class SparkScan(spark: SparkSession,
                     hoodieTableName: String,
                     selectedPartitions: Seq[PartitionDirectory],
                     tableSchema: StructType,
                     partitionSchema: StructType,
                     requiredSchema: StructType,
                     filters: Seq[Filter],
                     options: Map[String, String],
                     @transient hadoopConf: Configuration)
  extends SparkBatch (
    spark,
    selectedPartitions,
    partitionSchema,
    requiredSchema,
    filters,
    options,
    hadoopConf) with Scan with SupportsRuntimeFiltering {

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def toBatch: Batch = this

  override def description(): String = {
    hoodieTableName + ", PushedFilters: " + filters.mkString("[", ", ", "], ")
  }

  override def filterAttributes(): Array[NamedReference] = {
    val scanFields = readSchema().fields.map(_.name).toSet

    val namedReference = partitionSchema.fields.filter(field => scanFields.contains(field.name))
      .map(field => Expressions.column(field.name))
    namedReference
  }

  override def filter(filters: Array[Filter]): Unit = {
    // TODO need to filter out irrelevant data files
  }

}
