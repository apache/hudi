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
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.isMetadataTable
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.io.storage.HoodieHFileReader
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String)

case class HoodieTableState(recordKeyField: String,
                            preCombineFieldOpt: Option[String])

/**
 * Hoodie BaseRelation which extends [[PrunedFilteredScan]].
 */
abstract class HoodieBaseRelation(val sqlContext: SQLContext,
                                  metaClient: HoodieTableMetaClient,
                                  optParams: Map[String, String],
                                  userSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan with Logging {

  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected lazy val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
  protected lazy val jobConf = new JobConf(conf)

  // If meta fields are enabled, always prefer key from the meta field as opposed to user-specified one
  // NOTE: This is historical behavior which is preserved as is
  protected lazy val recordKeyField: String =
    if (metaClient.getTableConfig.populateMetaFields()) HoodieRecord.RECORD_KEY_METADATA_FIELD
    else metaClient.getTableConfig.getRecordKeyFieldProp

  protected lazy val preCombineFieldOpt: Option[String] = getPrecombineFieldProperty

  /**
   * @VisibleInTests
   */
  lazy val mandatoryColumns: Seq[String] = {
    if (isMetadataTable(metaClient)) {
      Seq(HoodieMetadataPayload.KEY_FIELD_NAME, HoodieMetadataPayload.SCHEMA_FIELD_NAME_TYPE)
    } else {
      Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())
    }
  }

  protected lazy val specifiedQueryInstant: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  protected lazy val tableAvroSchema: Schema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    Try(schemaUtil.getTableAvroSchema).getOrElse(
      // If there is no commit in the table, we can't get the schema
      // t/h [[TableSchemaResolver]], fallback to the provided [[userSchema]] instead.
      userSchema match {
        case Some(s) => SchemaConverters.toAvroType(s)
        case _ => throw new IllegalArgumentException("User-provided schema is required in case the table is empty")
      }
    )
  }

  protected val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = metaClient.getTableConfig.getPartitionFields.orElse(Array.empty)

  protected def getPrecombineFieldProperty: Option[String] =
    Option(metaClient.getTableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  override def schema: StructType = tableStructSchema

  /**
   * This method controls whether relation will be producing
   * <ul>
   *   <li>[[Row]], when it's being equal to true</li>
   *   <li>[[InternalRow]], when it's being equal to false</li>
   * </ul>
   *
   * Returning [[InternalRow]] directly enables us to save on needless ser/de loop from [[InternalRow]] (being
   * produced by file-reader) to [[Row]] and back
   */
  override final def needConversion: Boolean = false

  /**
   * NOTE: DO NOT OVERRIDE THIS METHOD
   */
  override final def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Here we rely on a type erasure, to workaround inherited API restriction and pass [[RDD[InternalRow]]] back as [[RDD[Row]]]
    // Please check [[needConversion]] scala-doc for more details
    doBuildScan(requiredColumns, filters).asInstanceOf[RDD[Row]]
  }

  protected def doBuildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow]

  protected final def appendMandatoryColumns(requestedColumns: Array[String]): Array[String] = {
    val missing = mandatoryColumns.filter(col => !requestedColumns.contains(col))
    requestedColumns ++ missing
  }
}

object HoodieBaseRelation {

  def isMetadataTable(metaClient: HoodieTableMetaClient) =
    HoodieTableMetadata.isMetadataTable(metaClient.getBasePath)

  /**
   * Returns file-reader routine accepting [[PartitionedFile]] and returning an [[Iterator]]
   * over [[InternalRow]]
   */
  def createBaseFileReader(spark: SparkSession,
                           partitionSchema: StructType,
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hfileReader = createHFileReader(
      spark = spark,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )
    val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = spark,
      dataSchema = tableSchema.structTypeSchema,
      partitionSchema = partitionSchema,
      requiredSchema = requiredSchema.structTypeSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )

    partitionedFile => {
      val extension = FSUtils.getFileExtension(partitionedFile.filePath)
      if (HoodieFileFormat.PARQUET.getFileExtension.equals(extension)) {
        parquetReader.apply(partitionedFile)
      } else if (HoodieFileFormat.HFILE.getFileExtension.equals(extension)) {
        hfileReader.apply(partitionedFile)
      } else {
        throw new UnsupportedOperationException(s"Base file format not supported by Spark DataSource ($partitionedFile)")
      }
    }
  }

  private def createHFileReader(spark: SparkSession,
                                tableSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                filters: Seq[Filter],
                                options: Map[String, String],
                                hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hadoopConfBroadcast =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    partitionedFile => {
      val hadoopConf = hadoopConfBroadcast.value.get()
      val reader = new HoodieHFileReader[GenericRecord](hadoopConf, new Path(partitionedFile.filePath),
        new CacheConfig(hadoopConf))

      val requiredRowSchema = requiredSchema.structTypeSchema
      // NOTE: Schema has to be parsed at this point, since Avro's [[Schema]] aren't serializable
      //       to be passed from driver to executor
      val requiredAvroSchema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
      val avroToRowConverter = AvroConversionUtils.createAvroToInternalRowConverter(requiredAvroSchema, requiredRowSchema)

      reader.getRecordIterator(requiredAvroSchema).asScala
        .map(record => {
          avroToRowConverter.apply(record.asInstanceOf[GenericRecord]).get
        })
    }
  }
}
