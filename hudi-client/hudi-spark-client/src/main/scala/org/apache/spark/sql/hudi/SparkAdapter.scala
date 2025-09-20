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

package org.apache.spark.sql.hudi

import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping}
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSchemaConverters, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.parser.HoodieExtendedParserInterface
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Connection, ResultSet}
import java.util.{Locale, TimeZone}

/**
 * Interface adapting discrepancies and incompatibilities between different Spark versions
 */
trait SparkAdapter extends Serializable {

  /**
   * Checks whether provided instance of [[InternalRow]] is actually an instance of [[ColumnarBatchRow]]
   */
  def isColumnarBatchRow(r: InternalRow): Boolean

  /**
   * Creates Catalyst [[Metadata]] for Hudi's meta-fields (designating these w/
   * [[METADATA_COL_ATTR_KEY]] if available (available in Spark >= 3.2)
   */
  def createCatalystMetadataForMetaField: Metadata

  /**
   * Inject table-valued functions to SparkSessionExtensions
   */
  def injectTableFunctions(extensions: SparkSessionExtensions): Unit = {}

  /**
   * Returns an instance of [[HoodieCatalystExpressionUtils]] providing for common utils operating
   * on Catalyst [[Expression]]s
   */
  def getCatalystExpressionUtils: HoodieCatalystExpressionUtils

  /**
   * Returns an instance of [[HoodieCatalystPlansUtils]] providing for common utils operating
   * on Catalyst [[LogicalPlan]]s
   */
  def getCatalystPlanUtils: HoodieCatalystPlansUtils

  /**
   * Returns an instance of [[HoodieSchemaUtils]] providing schema utils.
   */
  def getSchemaUtils: HoodieSchemaUtils

  /**
   * Creates instance of [[HoodieAvroSerializer]] providing for ability to serialize
   * Spark's [[InternalRow]] into Avro payloads
   */
  def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer

  /**
   * Creates instance of [[HoodieAvroDeserializer]] providing for ability to deserialize
   * Avro payloads into Spark's [[InternalRow]]
   */
  def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer

  /**
   * Creates instance of [[HoodieAvroSchemaConverters]] allowing to convert b/w Avro and Catalyst schemas
   */
  def getAvroSchemaConverters: HoodieAvroSchemaConverters

  /**
   * Create the SparkRowSerDe.
   */
  def createSparkRowSerDe(schema: StructType): SparkRowSerDe

  /**
   * Create the hoodie's extended spark sql parser.
   */
  def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface

  /**
   * Create the SparkParsePartitionUtil.
   */
  def getSparkParsePartitionUtil: SparkParsePartitionUtil

  /**
   * Gets the [[HoodieSparkPartitionedFileUtils]].
   */
  def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils

  /**
   * Get the [[DateFormatter]].
   */
  def getDateFormatter(tz: TimeZone): DateFormatter

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  def getFilePartitions(sparkSession: SparkSession, partitionedFiles: Seq[PartitionedFile],
                        maxSplitBytes: Long): Seq[FilePartition]

  /**
   * Checks whether [[LogicalPlan]] refers to Hudi table, and if it's the case extracts
   * corresponding [[CatalogTable]]
   */
  def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable]

  def isHoodieTable(map: java.util.Map[String, String]): Boolean = {
    isHoodieTable(map.getOrDefault("provider", ""))
  }

  def isHoodieTable(table: CatalogTable): Boolean = {
    isHoodieTable(table.provider.map(_.toLowerCase(Locale.ROOT)).orNull)
  }

  def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHoodieTable(table)
  }

  def isHoodieTable(provider: String): Boolean = {
    "hudi".equalsIgnoreCase(provider)
  }

  /**
   * Create instance of [[ParquetFileFormat]]
   */
  def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat]

  def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch

  /**
   * Create instance of [[InterpretedPredicate]]
   *
   * TODO move to HoodieCatalystExpressionUtils
   */
  def createInterpretedPredicate(e: Expression): InterpretedPredicate

  /**
   * Create Hoodie relation based on globPaths, otherwise use tablePath if it's empty
   */
  def createRelation(sqlContext: SQLContext,
                     metaClient: HoodieTableMetaClient,
                     schema: Schema,
                     globPaths: Array[StoragePath],
                     parameters: java.util.Map[String, String]): BaseRelation

  /**
   * Create instance of [[HoodieFileScanRDD]]
   * SPARK-37273 FileScanRDD constructor changed in SPARK 3.3
   */
  def createHoodieFileScanRDD(sparkSession: SparkSession,
                              readFunction: PartitionedFile => Iterator[InternalRow],
                              filePartitions: Seq[FilePartition],
                              readDataSchema: StructType,
                              metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD

  /**
   * Extract condition in [[DeleteFromTable]]
   * SPARK-38626 condition is no longer Option in Spark 3.3
   */
  def extractDeleteCondition(deleteFromTable: Command): Expression

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  def convertStorageLevelToString(level: StorageLevel): String

  /**
   * Tries to translate a Catalyst Expression into data source Filter
   */
  def translateFilter(predicate: Expression, supportNestedPredicatePushdown: Boolean = false): Option[Filter]

  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return parquet file reader
   */
  def createParquetFileReader(vectorized: Boolean,
                              sqlConf: SQLConf,
                              options: Map[String, String],
                              hadoopConf: Configuration): SparkColumnarFileReader

  def createOrcFileReader(vectorized: Boolean,
                          sqlConf: SQLConf,
                          options: Map[String, String],
                          hadoopConf: Configuration,
                          dataSchema: StructType): SparkColumnarFileReader

  /**
   * use new qe execute
   */
  def sqlExecutionWithNewExecutionId[T](sparkSession: SparkSession,
                                        queryExecution: QueryExecution,
                                        name: Option[String] = None)(body: => T): T


  /**
   * Stop spark context with exit code
   *
   * @param jssc JavaSparkContext object to shutdown the spark context
   * @param exitCode passed as a param to shutdown spark context with provided exit code
   * @return
   */
  def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit

  def getDateTimeRebaseMode(): Object

  def isLegacyBehaviorPolicy(value: Object): Boolean

  def isTimestampNTZType(dataType: DataType): Boolean

  /**
   * Takes a [[ResultSet]] and returns its Catalyst schema.
   *
   * @param conn           [[Connection]] instance.
   * @param resultSet      [[ResultSet]] instance.
   * @param dialect        [[JdbcDialect]] instance.
   * @param alwaysNullable If true, all the columns are nullable.
   * @return A [[StructType]] giving the Catalyst schema.
   */
  def getSchema(conn: Connection,
                resultSet: ResultSet,
                dialect: JdbcDialect,
                alwaysNullable: Boolean = false,
                isTimestampNTZ: Boolean = false): StructType

  /**
   * Gets the [[UTF8String]] factory implementation for the current Spark version.
   * [SPARK-46832] [[UTF8String]] doesn't support compareTo anymore since Spark 4.0
   *
   * @return [[HoodieUTF8StringFactory]] instance
   */
  def getUTF8StringFactory: HoodieUTF8StringFactory

  /**
   * Creates a [[HoodieInternalRow]] with meta fields.
   *
   * @param metaFields               Array of [[UTF8String]] meta fields to prepend
   * @param sourceRow                The source [[InternalRow]] containing the data
   * @param sourceContainsMetaFields Whether the source row already contains meta fields
   * @return A new [[HoodieInternalRow]] with appropriate meta fields handling
   */
  def createInternalRow(metaFields: Array[UTF8String],
                        sourceRow: InternalRow,
                        sourceContainsMetaFields: Boolean): HoodieInternalRow

  /**
   * Creates a partition-to-CDC file group mapping for CDC operations.
   *
   * @param partitionValues [[InternalRow]] containing the partition values
   * @param fileSplits      List of CDC file splits for this partition
   * @return [[HoodiePartitionCDCFileGroupMapping]] instance
   */
  def createPartitionCDCFileGroupMapping(partitionValues: InternalRow,
                                         fileSplits: List[HoodieCDCFileSplit]): HoodiePartitionCDCFileGroupMapping

  /**
   * Creates a partition-to-file slice mapping for file operations.
   *
   * @param values [[InternalRow]] containing the partition values
   * @param slices Map of file IDs to FileSlice instances
   * @return [[HoodiePartitionFileSliceMapping]] instance
   */
  def createPartitionFileSliceMapping(values: InternalRow,
                                      slices: Map[String, FileSlice]): HoodiePartitionFileSliceMapping

  /**
   * Creates a [[ParseException]] with proper location information.
   *
   * @param command   Optional command string that caused the exception
   * @param exception The underlying AnalysisException
   * @param start     Start position in the input where the error occurred
   * @param stop      End position in the input where the error occurred
   * @return A new [[ParseException]] with location details
   */
  def newParseException(command: Option[String],
                        exception: AnalysisException,
                        start: Origin,
                        stop: Origin): ParseException

  /**
   * Splits files in a partition directory based on size constraints.
   *
   * @param sparkSession       The active Spark session
   * @param partitionDirectory The partition directory containing files to split
   * @param isSplitable        Whether the files can be split
   * @param maxSplitSize       Maximum size for each split in bytes
   * @return Sequence of [[PartitionedFile]] instances after splitting
   */
  def splitFiles(sparkSession: SparkSession,
                 partitionDirectory: PartitionDirectory,
                 isSplitable: Boolean,
                 maxSplitSize: Long): Seq[PartitionedFile]

  /**
   * Creates a [[Column]] from a Catalyst [[Expression]].
   *
   * @param expression The Catalyst [[Expression]] to convert
   * @return A Spark SQL [[Column]] wrapping the expression
   */
  def createColumnFromExpression(expression: Expression): Column

  /**
   * Extracts the underlying [[Expression]] from a [[Column]].
   *
   * @param column The Spark SQL [[Column]]
   * @return The underlying Catalyst [[Expression]]
   */
  def getExpressionFromColumn(column: Column): Expression

  /**
   * Gets the unsafe utilities for low-level memory operations.
   *
   * @return [[HoodieUnsafeUtils]] instance
   */
  def getUnsafeUtils: HoodieUnsafeUtils

  /**
   * Gets the DataFrame utility helper for the current Spark version.
   * Since Spark 4.0 [[org.apache.spark.sql.classic.Dataset]] has to be used.
   *
   * @return [[DataFrameUtil]] instance
   */
  def getDataFrameUtil: DataFrameUtil

  /**
   * Creates a [[DataFrame]] from an RDD of [[InternalRow]]s.
   *
   * @param spark       The active Spark session
   * @param rdd         RDD containing InternalRow instances
   * @param schema      The schema for the [[DataFrame]]
   * @param isStreaming Whether this is a streaming DataFrame
   * @return A new [[DataFrame]] with the specified schema
   */
  def internalCreateDataFrame(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType, isStreaming: Boolean = false): DataFrame

  /**
   * Creates a streaming [[DataFrame]] from a [[HadoopFsRelation.]]
   *
   * @param sqlContext     The SQL context
   * @param relation       The [[HadoopFsRelation]] to stream from
   * @param requiredSchema The required schema for the streaming DataFrame
   * @return A streaming [[DataFrame]]
   */
  def createStreamingDataFrame(sqlContext: SQLContext, relation: HadoopFsRelation, requiredSchema: StructType): DataFrame
}
