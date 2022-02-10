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
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hudi.AvroConversionUtils.convertStructTypeToAvroSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.io.storage.HoodieHFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.util.Try

case class HoodieTableSchemas(tableSchema: StructType,
                              partitionSchema: StructType,
                              requiredSchema: StructType,
                              tableAvroSchema: String,
                              requiredAvroSchema: String)

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

object HoodieBaseRelation {

  /**
   * TODO
   */
  def createBaseFileReader(spark: SparkSession,
                           tableSchemas: HoodieTableSchemas,
                           filters: Array[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hfileReader = createHFileReader(
      spark = spark,
      tableSchemas = tableSchemas,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )
    val parquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = spark,
      dataSchema = tableSchemas.tableSchema,
      partitionSchema = tableSchemas.partitionSchema,
      requiredSchema = tableSchemas.requiredSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )

    partitionedFile => {
      if (partitionedFile.filePath.endsWith(".parquet"))
        parquetReader.apply(partitionedFile)
      else if (partitionedFile.filePath.endsWith(".hfile"))
        hfileReader.apply(partitionedFile)
      else
        throw new UnsupportedOperationException(s"Base file format not supported by Spark DataSource ($partitionedFile)")
    }
  }

  private def createHFileReader(spark: SparkSession,
                                tableSchemas: HoodieTableSchemas,
                                filters: Array[Filter],
                                options: Map[String, String],
                                hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hadoopConfBroadcast =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val requiredSchema = tableSchemas.requiredSchema
    val requiredAvroSchema = new Schema.Parser().parse(tableSchemas.requiredAvroSchema)

    partitionedFile => {
      val hadoopConf = hadoopConfBroadcast.value.value
      val reader = new HoodieHFileReader[GenericRecord](hadoopConf, new Path(partitionedFile.filePath),
        new CacheConfig(hadoopConf))

      // TODO
      //      val readerSchema = convertStructTypeToAvroSchema(requiredSchema, "MetadataProjectedRecord", "org.apache.hoodie")

      val tableAvroSchema = convertStructTypeToAvroSchema(tableSchema, "MetadataRecord", "org.apache.hoodie")
      val avroToRowConverter = AvroConversionUtils.createAvroToRowConverter(tableAvroSchema, tableSchema)

      reader.getRecordIterator(tableAvroSchema).asScala
        .map(record => {
          // TODO projection
          avroToRowConverter.apply(record).get.asInstanceOf[InternalRow]
        })
    }
  }

}