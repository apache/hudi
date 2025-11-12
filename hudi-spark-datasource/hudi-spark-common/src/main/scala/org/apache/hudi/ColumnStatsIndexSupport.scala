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

import org.apache.hudi.ColumnStatsIndexSupport._
import org.apache.hudi.HoodieCatalystUtils.{withPersistedData, withPersistedDataset}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.model._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.function.SerializableFunction
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.BinaryUtil.toBytes
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{ColumnStatsIndexPrefixRawKey, HoodieIndexVersion, HoodieMetadataPayload, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getValidIndexedColumns
import org.apache.hudi.stats.{SparkValueMetadataUtils, ValueMetadata, ValueType}
import org.apache.hudi.stats.ValueMetadata.getValueMetadata
import org.apache.hudi.util.JFunction

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashMap

class ColumnStatsIndexSupport(spark: SparkSession,
                              tableSchema: StructType,
                              avroSchema: Schema,
                              @transient metadataConfig: HoodieMetadataConfig,
                              @transient metaClient: HoodieTableMetaClient,
                              allowCaching: Boolean = false)
  extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  @transient private lazy val cachedColumnStatsIndexViews: ParHashMap[Seq[String], DataFrame] = ParHashMap()

  // NOTE: Since [[metadataConfig]] is transient this has to be eagerly persisted, before this will be passed
  //       on to the executor
  protected val inMemoryProjectionThreshold = metadataConfig.getColumnStatsIndexInMemoryProjectionThreshold

  private lazy val indexedColumns: Set[String] = getIndexedColsWithColStats(metaClient)

  override def getIndexName: String = ColumnStatsIndexSupport.INDEX_NAME

  def getIndexedColsWithColStats(metaClient: HoodieTableMetaClient) : Set[String] = {
    val indexDefOpt = metaClient.getIndexForMetadataPartition(PARTITION_NAME_COLUMN_STATS)
    if (indexDefOpt.isPresent) {
      indexDefOpt.get().getSourceFields.asScala.toSet
    } else {
      Set.empty[String]
    }
  }

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    if (isIndexAvailable && queryFilters.nonEmpty && queryReferencedColumns.nonEmpty) {
      val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
      val (prunedPartitions, prunedFileNames) = getPrunedPartitionsAndFileNames(fileIndex, prunedPartitionsAndFileSlices)
      // NOTE: If partition pruning doesn't prune any files, then there's no need to apply file filters
      //       when loading the Column Statistics Index
      val prunedFileNamesOpt = if (shouldPushDownFilesFilter) Some(prunedFileNames) else None
      val getValidIndexedColumnsFunc: HoodieIndexDefinition => Seq[String] = { indexDefinition =>
        getValidIndexedColumns(indexDefinition, avroSchema, metaClient.getTableConfig).asScala.toSeq
      }
      loadTransposed(queryReferencedColumns, readInMemory, Some(prunedPartitions), prunedFileNamesOpt) { transposedColStatsDF =>
        Some(getCandidateFiles(transposedColStatsDF, queryFilters, prunedFileNames, getValidIndexedColumnsFunc))
      }
    } else {
      Option.empty
    }
  }

  override def invalidateCaches(): Unit = {
    cachedColumnStatsIndexViews.foreach { case (_, df) => df.unpersist() }
    cachedColumnStatsIndexViews.clear()
  }

  /**
   * Returns true in cases when Column Stats Index is built and available as standalone partition
   * w/in the Metadata Table
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled &&
      metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
  }

  /**
   * Loads view of the Column Stats Index in a transposed format where single row coalesces every columns'
   * statistics for a single file, returning it as [[DataFrame]]
   *
   * Please check out scala-doc of the [[transpose]] method explaining this view in more details
   */
  def loadTransposed[T](targetColumns: Seq[String],
                        shouldReadInMemory: Boolean,
                        prunedPartitions: Option[Set[String]] = None,
                        prunedFileNamesOpt: Option[Set[String]] = None)(block: DataFrame => T): T = {
    cachedColumnStatsIndexViews.get(targetColumns) match {
      case Some(cachedDF) =>
        block(cachedDF)
      case None =>
        val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = prunedFileNamesOpt match {
          case Some(prunedFileNames) =>
            val filterFunction = new SerializableFunction[HoodieMetadataColumnStats, java.lang.Boolean] {
              override def apply(r: HoodieMetadataColumnStats): java.lang.Boolean = {
                prunedFileNames.contains(r.getFileName)
              }
            }
            loadColumnStatsIndexRecords(targetColumns, prunedPartitions, shouldReadInMemory).filter(filterFunction)
          case None =>
            loadColumnStatsIndexRecords(targetColumns, prunedPartitions, shouldReadInMemory)
        }

        withPersistedData(colStatsRecords, StorageLevel.MEMORY_ONLY) {
          val (transposedRows, indexSchema) = transpose(colStatsRecords, targetColumns)
          val df = if (shouldReadInMemory) {
            // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
            //       of the transposed table in memory, facilitating execution of the subsequently chained operations
            //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
            //       Optimizer)
            sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, transposedRows.collectAsList().asScala.toSeq, indexSchema)
          } else {
            val rdd = HoodieJavaRDD.getJavaRDD(transposedRows)
            spark.createDataFrame(rdd, indexSchema)
          }

          if (allowCaching) {
            cachedColumnStatsIndexViews.put(targetColumns, df)
            // NOTE: Instead of collecting the rows from the index and hold them in memory, we instead rely
            //       on Spark as (potentially distributed) cache managing data lifecycle, while we simply keep
            //       the referenced to persisted [[DataFrame]] instance
            df.persist(StorageLevel.MEMORY_ONLY)

            block(df)
          } else {
            withPersistedDataset(df) {
              block(df)
            }
          }
        }
    }
  }

  /**
   * Loads a view of the Column Stats Index in a raw format, returning it as [[DataFrame]]
   *
   * Please check out scala-doc of the [[transpose]] method explaining this view in more details
   */
  def load(targetColumns: Seq[String] = Seq.empty, shouldReadInMemory: Boolean = false): DataFrame = {
    // NOTE: If specific columns have been provided, we can considerably trim down amount of data fetched
    //       by only fetching Column Stats Index records pertaining to the requested columns.
    //       Otherwise we fallback to read whole Column Stats Index
    if (targetColumns.nonEmpty) {
      loadColumnStatsIndexForColumnsInternal(targetColumns, shouldReadInMemory)
    } else {
      loadFullColumnStatsIndexInternal()
    }
  }

  /**
   * Transposes and converts the raw table format of the Column Stats Index representation,
   * where each row/record corresponds to individual (column, file) pair, into the table format
   * where each row corresponds to single file with statistic for individual columns collated
   * w/in such row:
   *
   * Metadata Table Column Stats Index format:
   *
   * <pre>
   *  +---------------------------+------------+------------+------------+-------------+
   *  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
   *  +---------------------------+------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          A |          1 |         10 |           0 |
   *  | another_base_file.parquet |          A |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+------------+-------------+
   * </pre>
   *
   * Returned table format
   *
   * <pre>
   *  +---------------------------+------------+------------+-------------+
   *  |          file             | A_minValue | A_maxValue | A_nullCount |
   *  +---------------------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          1 |         10 |           0 |
   *  | another_base_file.parquet |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+-------------+
   * </pre>
   *
   * NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
   *       query at hand might only be referencing a handful of those. As such, we collect all the
   *       column references from the filtering expressions, and only transpose records corresponding to the
   *       columns referenced in those
   *
   * @param colStatsRecords [[HoodieData[HoodieMetadataColumnStats]]] bearing raw Column Stats Index records
   * @param queryColumns target columns to be included into the final table
   * @return reshaped table according to the format outlined above
   */
  private def transpose(colStatsRecords: HoodieData[HoodieMetadataColumnStats], queryColumns: Seq[String]): (HoodieData[Row], StructType) = {
    val tableSchemaFieldMap = tableSchema.fields.map(f => (f.name, f)).toMap
    // NOTE: We're sorting the columns to make sure final index schema matches layout
    //       of the transposed table
    val sortedTargetColumnsSet = TreeSet(queryColumns:_*)

    val sortedTargetColDataTypeMap = sortedTargetColumnsSet.toSeq.map(fieldName => (fieldName, HoodieSchemaUtils.getSchemaForField(tableSchema, fieldName).getValue)).toMap

    // NOTE: This is a trick to avoid pulling all of [[ColumnStatsIndexSupport]] object into the lambdas'
    //       closures below
    val indexedColumns = this.indexedColumns

    // NOTE: It's crucial to maintain appropriate ordering of the columns
    //       matching table layout: hence, we cherry-pick individual columns
    //       instead of simply filtering in the ones we're interested in the schema
    val (indexSchema, targetIndexedColumns) = composeIndexSchema(sortedTargetColumnsSet.toSeq, indexedColumns.toSeq, tableSchema)

    // Here we perform complex transformation which requires us to modify the layout of the rows
    // of the dataset, and therefore we rely on low-level RDD API to avoid incurring encoding/decoding
    // penalty of the [[Dataset]], since it's required to adhere to its schema at all times, while
    // RDDs are not;
    val useJava8api = spark.sessionState.conf.datetimeJava8ApiEnabled
    val transposedRows: HoodieData[Row] = colStatsRecords
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      .filter(JFunction.toJavaSerializableFunction(r => sortedTargetColumnsSet.contains(r.getColumnName)))
      .mapToPair(JFunction.toJavaSerializableFunctionPairOut(r => {
        if (r.getMinValue == null && r.getMaxValue == null) {
          // Corresponding row could be null in either of the 2 cases
          //    - Column contains only null values (in that case both min/max have to be nulls)
          //    - This is a stubbed Column Stats record (used as a tombstone)
          collection.Pair.of(r.getFileName, r)
        } else {
          val minValueWrapper = r.getMinValue
          val maxValueWrapper = r.getMaxValue

          checkState(minValueWrapper != null && maxValueWrapper != null, "Invalid Column Stats record: either both min/max have to be null, or both have to be non-null")

          val colName = r.getColumnName
          val colType = sortedTargetColDataTypeMap(colName).dataType

          val valueMetadata = getValueMetadata(r.getValueType)
          val minValue = extractColStatsValue(minValueWrapper, colType, valueMetadata, useJava8api)
          val maxValue = extractColStatsValue(maxValueWrapper, colType, valueMetadata, useJava8api)

          // Update min-/max-value structs w/ unwrapped values in-place
          r.setMinValue(minValue)
          r.setMaxValue(maxValue)

          collection.Pair.of(r.getFileName, r)
        }
      }))
      .groupByKey()
      .map(JFunction.toJavaSerializableFunction(p => {
        val columnRecordsSeq: Seq[HoodieMetadataColumnStats] = p.getValue.asScala.toSeq
        val fileName: String = p.getKey
        val valueCount: Long = columnRecordsSeq.head.getValueCount

        // To properly align individual rows (corresponding to a file) w/in the transposed projection, we need
        // to align existing column-stats for individual file with the list of expected ones for the
        // whole transposed projection (a superset of all files)
        val columnRecordsMap = columnRecordsSeq.map(r => (r.getColumnName, r)).toMap
        val alignedColStatRecordsSeq = targetIndexedColumns.map(columnRecordsMap.get)

        val coalescedRowValuesSeq =
          alignedColStatRecordsSeq.foldLeft(ListBuffer[Any](fileName, valueCount)) {
            case (acc, opt) =>
              opt match {
                case Some(colStatRecord) =>
                  acc ++= Seq(colStatRecord.getMinValue, colStatRecord.getMaxValue, colStatRecord.getNullCount)
                case None =>
                  // NOTE: This could occur in either of the following cases:
                  //    1. When certain columns exist in the schema but are absent in some data files due to
                  //       schema evolution or other reasons, these columns will not be present in the column stats.
                  //       In this case, we fill in default values by setting the min, max and null-count to null
                  //       (this behavior is consistent with reading non-existent columns from Parquet).
                  //    2. When certain columns are present both in the schema and the data files,
                  //       but the column stats are absent for these columns due to their types not supporting indexing,
                  //       we also set these columns to default values.
                  //
                  // This approach prevents errors during data skipping and, because the filter includes an isNull check,
                  // these conditions will not affect the accurate return of files from data skipping.
                  acc ++= Seq(null, null, null)
              }
          }

        Row(coalescedRowValuesSeq.toSeq: _*)
      }))

    (transposedRows, indexSchema)
  }

  private def loadColumnStatsIndexForColumnsInternal(targetColumns: Seq[String], shouldReadInMemory: Boolean): DataFrame = {
    val colStatsDF = {
      val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadColumnStatsIndexRecords(targetColumns, Option.empty, shouldReadInMemory)
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      val catalystRows: HoodieData[InternalRow] = colStatsRecords.mapPartitions(JFunction.toJavaSerializableFunction(it => {
        val converter = AvroConversionUtils.createAvroToInternalRowConverter(HoodieMetadataColumnStats.SCHEMA$, columnStatsRecordStructType)
        it.asScala.map(r => converter(r).orNull).asJava
      }), false)

      if (shouldReadInMemory) {
        // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
        //       of the transposed table in memory, facilitating execution of the subsequently chained operations
        //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
        //       Optimizer)
        sparkAdapter.getUnsafeUtils.createDataFrameFromInternalRows(spark, catalystRows.collectAsList().asScala.toSeq, columnStatsRecordStructType)
      } else {
        sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(spark, HoodieJavaRDD.getJavaRDD(catalystRows), columnStatsRecordStructType)
      }
    }

    colStatsDF.select(targetColumnStatsIndexColumns.map(col): _*)
  }

  def loadColumnStatsIndexRecords(targetColumns: Seq[String], prunedPartitions: Option[Set[String]] = None, shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // Read Metadata Table's Column Stats Index records into [[HoodieData]] container by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Extracting [[HoodieMetadataColumnStats]] records
    //    - Filtering out nulls
    checkState(targetColumns.nonEmpty)

    // Create raw key prefixes based on column names and optional partition names
    val rawKeys = if (prunedPartitions.isDefined) {
      val partitionsList = prunedPartitions.get.toList
      targetColumns.flatMap(colName =>
        partitionsList.map(partitionPath => new ColumnStatsIndexPrefixRawKey(colName, partitionPath))
      )
    } else {
      targetColumns.map(colName => new ColumnStatsIndexPrefixRawKey(colName))
    }

    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(
        HoodieListData.eager(rawKeys.asJava), HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, shouldReadInMemory)

    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }

  private def loadFullColumnStatsIndexInternal(): DataFrame = {
    val metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath)
    // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]]
    val colStatsDF = spark.read.format("org.apache.hudi")
      .options(metadataConfig.getProps.asScala)
      .load(s"$metadataTablePath/${MetadataPartitionType.COLUMN_STATS.getPartitionPath}")

    val requiredIndexColumns =
      targetColumnStatsIndexColumns.map(colName =>
        col(s"${HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS}.${colName}"))

    colStatsDF.where(col(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).isNotNull)
      .select(requiredIndexColumns: _*)
  }
}

object ColumnStatsIndexSupport {
  val INDEX_NAME = "COLUMN_STATS"

  private val expectedAvroSchemaValues = Set("BooleanWrapper", "IntWrapper", "LongWrapper", "FloatWrapper", "DoubleWrapper",
    "BytesWrapper", "StringWrapper", "DateWrapper", "DecimalWrapper", "TimeMicrosWrapper", "TimestampMicrosWrapper")

  /**
   * Target Column Stats Index columns which internally are mapped onto fields of the corresponding
   * Column Stats record payload ([[HoodieMetadataColumnStats]]) persisted w/in Metadata Table
   */
  private val targetColumnStatsIndexColumns = Seq(
    HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME
  )

  private val columnStatsRecordStructType: StructType = AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataColumnStats.SCHEMA$)

  /**
   * @VisibleForTesting
   */
  def composeIndexSchema(targetColumnNames: Seq[String], indexedColumns: Seq[String], tableSchema: StructType): (StructType, Seq[String]) = {
    val fileNameField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME, StringType, nullable = true, Metadata.empty)
    val valueCountField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT, LongType, nullable = true, Metadata.empty)

    val targetIndexedColumns = targetColumnNames.filter(indexedColumns.contains(_))
    val targetIndexedFields = targetIndexedColumns.map(colName => HoodieSchemaUtils.getSchemaForField(tableSchema, colName))

    (StructType(
      targetIndexedFields.foldLeft(Seq(fileNameField, valueCountField)) {
        case (acc, field) =>
          acc ++ Seq(
            composeColumnStatStructType(field.getKey, HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, field.getValue.dataType),
            composeColumnStatStructType(field.getKey, HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, field.getValue.dataType),
            composeColumnStatStructType(field.getKey, HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, LongType))
      }
    ), targetIndexedColumns)
  }

  @inline def getMinColumnNameFor(colName: String): String =
    formatColName(colName, HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE)

  @inline def getMaxColumnNameFor(colName: String): String =
    formatColName(colName, HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE)

  @inline def getNullCountColumnNameFor(colName: String): String =
    formatColName(colName, HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT)

  @inline def getValueCountColumnNameFor: String =
    HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT

  @inline private def formatColName(col: String, statName: String) = { // TODO add escaping for
    String.format("%s_%s", col, statName)
  }

  @inline def composeColumnStatStructType(col: String, statName: String, dataType: DataType) =
    StructField(formatColName(col, statName), dataType, nullable = true, Metadata.empty)

  def extractColStatsValue(valueWrapper: AnyRef, dataType: DataType, valueMetadata: ValueMetadata, useJava8api: Boolean): Any = {
    valueMetadata.getValueType match {
      case ValueType.V1 => extractWrapperValueV1(valueWrapper, dataType)
      case _ => extractColStatsValueV2(valueWrapper, dataType, valueMetadata, useJava8api)
    }
  }

  private def extractColStatsValueV2(valueWrapper: AnyRef, dataType: DataType, valueMetadata: ValueMetadata, useJava8api: Boolean): Any = {
    val colStatsValue = SparkValueMetadataUtils.convertJavaTypeToSparkType(SparkValueMetadataUtils.getValueMetadata(dataType, HoodieIndexVersion.V2)
      .standardizeJavaTypeAndPromote(valueMetadata.unwrapValue(valueWrapper)), useJava8api)
    // TODO: should this be done here? Should we handle this with adding more value types?
    // TODO: should this logic be in convertJavaTypeToSparkType?
    dataType match {
      case ShortType => colStatsValue.asInstanceOf[Int].toShort
      case ByteType => colStatsValue.asInstanceOf[Int].toByte
      case _ => colStatsValue
    }
  }

  def extractWrapperValueV1(valueWrapper: AnyRef, dataType: DataType): Any =
    deserialize(tryUnpackValueWrapper(valueWrapper), dataType)

  def tryUnpackValueWrapper(valueWrapper: AnyRef): Any = {
    valueWrapper match {
      case w: BooleanWrapper => w.getValue
      case w: IntWrapper => w.getValue
      case w: LongWrapper => w.getValue
      case w: FloatWrapper => w.getValue
      case w: DoubleWrapper => w.getValue
      case w: DecimalWrapper => w.getValue  // Moved above BytesWrapper to ensure proper matching
      case w: BytesWrapper => w.getValue
      case w: StringWrapper => w.getValue
      case w: DateWrapper => w.getValue
      case w: TimeMicrosWrapper => w.getValue
      case w: TimestampMicrosWrapper => w.getValue

      case r: GenericData.Record if expectedAvroSchemaValues.contains(r.getSchema.getName) =>
        r.get("value")

      case _ => throw new UnsupportedOperationException(s"Not recognized value wrapper type (${valueWrapper.getClass.getSimpleName})")
    }
  }

  val decConv = new DecimalConversion()

  def deserialize(value: Any, dataType: DataType): Any = {
    // Handle TimestampNTZType separately since it does not exist in version lower than spark3.5
    if (SparkAdapterSupport.sparkAdapter.isTimestampNTZType(dataType)) {
      DateTimeUtils.microsToLocalDateTime(value.asInstanceOf[Long])
    } else {
      dataType match {
        // NOTE: Since we can't rely on Avro's "date", and "timestamp-micros" logical-types, we're
        //       manually encoding corresponding values as int and long w/in the Column Stats Index and
        //       here we have to decode those back into corresponding logical representation.
        case TimestampType => DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long])
        case DateType => DateTimeUtils.toJavaDate(value.asInstanceOf[Int])
        // Standard types
        case StringType => value
        case BooleanType => value
        // Numeric types
        case FloatType => value
        case DoubleType => value
        case LongType => value
        case IntegerType => value
        // NOTE: All integral types of size less than Int are encoded as Ints in MT
        case ShortType => value.asInstanceOf[Int].toShort
        case ByteType => value.asInstanceOf[Int].toByte

        case dt: DecimalType =>
          value match {
            case buffer: ByteBuffer =>
              val logicalType = DecimalWrapper.SCHEMA$.getField("value").schema().getLogicalType
              decConv.fromBytes(buffer, null, logicalType).setScale(dt.scale, java.math.RoundingMode.UNNECESSARY)
            case bd: BigDecimal =>
              // Scala BigDecimal: convert to java.math.BigDecimal and enforce the scale
              bd.bigDecimal.setScale(dt.scale, java.math.RoundingMode.UNNECESSARY)
            case bd: java.math.BigDecimal =>
              bd.setScale(dt.scale, java.math.RoundingMode.UNNECESSARY)
            case other =>
              throw new UnsupportedOperationException(s"Cannot deserialize value for DecimalType: unexpected type ${other.getClass.getName}")
          }
        case BinaryType =>
          value match {
            case b: ByteBuffer => toBytes(b)
            case other => other
          }

        case _ =>
          throw new UnsupportedOperationException(s"Data type for the statistic value is not recognized $dataType")
      }
    }
  }
}
