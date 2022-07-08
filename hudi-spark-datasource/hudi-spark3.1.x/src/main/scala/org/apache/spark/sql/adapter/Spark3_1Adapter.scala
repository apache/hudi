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

package org.apache.spark.sql.adapter

import org.apache.avro.Schema
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer, HoodieSpark3_1AvroDeserializer, HoodieSpark3_1AvroSerializer}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark31HoodieParquetFileFormat}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, HoodieSpark3_1CatalystExpressionUtils, SparkSession}

/**
 * Implementation of [[SparkAdapter]] for Spark 3.1.x
 */
class Spark3_1Adapter extends BaseSpark3Adapter {

  override def createCatalystExpressionUtils(): HoodieCatalystExpressionUtils = HoodieSpark3_1CatalystExpressionUtils

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark3_1AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark3_1AvroDeserializer(rootAvroType, rootCatalystType)

  override def createHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark31HoodieParquetFileFormat(appendPartitionValues))
  }
}
