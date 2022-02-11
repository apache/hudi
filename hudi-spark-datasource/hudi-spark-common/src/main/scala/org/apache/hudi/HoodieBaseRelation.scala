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

import org.apache.avro.Schema

import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.sources.{BaseRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Hoodie BaseRelation which extends [[PrunedFilteredScan]].
 */
abstract class HoodieBaseRelation(
    val sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    optParams: Map[String, String],
    userSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan with Logging{

  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected val tableAvroSchema: Schema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    Try (schemaUtil.getTableAvroSchema).getOrElse(SchemaConverters.toAvroType(userSchema.get))
  }

  protected val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = metaClient.getTableConfig.getPartitionFields.orElse(Array.empty)

  override def schema: StructType = userSchema.getOrElse(tableStructSchema)

}
