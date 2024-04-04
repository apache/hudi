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
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.Spark30HoodieFileScanRDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer, HoodieSpark3_0AvroDeserializer, HoodieSpark3_0AvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, Spark30HoodieParquetReader, Spark30LegacyHoodieParquetFileFormat, SparkHoodieParquetReaderProperties}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, HoodieSpark30PartitionedFileUtils, HoodieSparkPartitionedFileUtils, PartitionedFile}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.parser.{HoodieExtendedParserInterface, HoodieSpark3_0ExtendedSqlParser}
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

/**
 * Implementation of [[SparkAdapter]] for Spark 3.0.x
 */
class Spark3_0Adapter extends BaseSpark3Adapter {

  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    super.resolveHoodieTable(plan).orElse {
      EliminateSubqueryAliases(plan) match {
        // First, we need to weed out unresolved plans
        case plan if !plan.resolved => None
        // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
        //       on top of it
        case PhysicalOperation(_, _, DataSourceV2Relation(table: CatalogTable, _, _, _, _)) if isHoodieTable(table) =>
          Some(table)
        case _ => None
      }
    }
  }

  override def isColumnarBatchRow(r: InternalRow): Boolean = ColumnarUtils.isColumnarBatchRow(r)

  def createCatalystMetadataForMetaField: Metadata =
  // NOTE: Since [[METADATA_COL_ATTR_KEY]] flag is not available in Spark 2.x,
  //       we simply produce an empty [[Metadata]] instance
    new MetadataBuilder().build()

  override def getCatalogUtils: HoodieSpark3CatalogUtils = HoodieSpark30CatalogUtils

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark30CatalystPlanUtils

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark30CatalystExpressionUtils

  override def getSchemaUtils: HoodieSchemaUtils = HoodieSpark30SchemaUtils

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils = HoodieSpark30PartitionedFileUtils

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark3_0AvroSerializer(rootCatalystType, rootAvroType, nullable)

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark3_0AvroDeserializer(rootAvroType, rootCatalystType)

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    new HoodieSpark3_0ExtendedSqlParser(spark, delegate)

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark30LegacyHoodieParquetFileFormat(appendPartitionValues))
  }

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD = {
    new Spark30HoodieFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def extractDeleteCondition(deleteFromTable: Command): Expression = {
    deleteFromTable.asInstanceOf[DeleteFromTable].condition.getOrElse(null)
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  override def convertStorageLevelToString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
    case MEMORY_ONLY => "MEMORY_ONLY"
    case MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case OFF_HEAP => "OFF_HEAP"
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $level")
  }

  /**
   * Get properties needed to read a parquet file
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return properties needed for reading a parquet file
   */
  override def getPropsForReadingParquet(vectorized: Boolean,
                                         sqlConf: SQLConf,
                                         options: Map[String, String],
                                         hadoopConf: Configuration): SparkHoodieParquetReaderProperties = {
    Spark30HoodieParquetReader.getPropsForReadingParquet(vectorized, sqlConf, options, hadoopConf)
  }

  /**
   * Read an individual parquet file
   *
   * @param file            parquet file to read
   * @param requiredSchema  desired output schema of the data
   * @param partitionSchema schema of the partition columns. Partition values will be appended to the end of every row
   * @param filters         filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param sharedConf      the hadoop conf
   * @param props           properties generated by [[getPropsForReadingParquet]] that are needed for reading
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def readParquetFile(file: PartitionedFile,
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               filters: Seq[sources.Filter],
                               sharedConf: Configuration,
                               props: SparkHoodieParquetReaderProperties): Iterator[InternalRow] = {
    Spark30HoodieParquetReader.readParquetFile(file, requiredSchema, partitionSchema, filters,
      new Configuration(sharedConf), props)
  }
}
