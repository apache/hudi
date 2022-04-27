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

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.ColumnStatsIndexSupport.{composeIndexSchema, deserialize, metadataRecordSchemaString, metadataRecordStructType, tryUnpackNonNullVal}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.model.HoodieMetadataRecord
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.ColumnIndexID
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, HoodieUnsafeCatalystUtils, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet

/**
 * Mixin trait abstracting away heavy-lifting of interactions with Metadata Table's Column Stats Index,
 * providing convenient interfaces to read it, transpose, etc
 */
trait ColumnStatsIndexSupport extends SparkAdapterSupport {

  def readColumnStatsIndex(spark: SparkSession,
                           tableBasePath: String,
                           metadataConfig: HoodieMetadataConfig,
                           targetColumns: Seq[String] = Seq.empty): DataFrame = {
    val targetColStatsIndexColumns = Seq(
      HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT)

    val requiredMetadataIndexColumns =
      (targetColStatsIndexColumns :+ HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME).map(colName =>
        s"${HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS}.${colName}")

    val metadataTableDF: DataFrame = {
      // NOTE: If specific columns have been provided, we can considerably trim down amount of data fetched
      //       by only fetching Column Stats Index records pertaining to the requested columns.
      //       Otherwise we fallback to read whole Column Stats Index
      if (targetColumns.nonEmpty) {
        readColumnStatsIndexForColumnsInternal(spark, targetColumns, metadataConfig, tableBasePath)
      } else {
        readFullColumnStatsIndexInternal(spark, metadataConfig, tableBasePath)
      }
    }

    val colStatsDF = metadataTableDF.where(col(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).isNotNull)
      .select(requiredMetadataIndexColumns.map(col): _*)

    colStatsDF
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
   * @param spark Spark session ref
   * @param colStatsDF [[DataFrame]] bearing raw Column Stats Index table
   * @param queryColumns target columns to be included into the final table
   * @param tableSchema schema of the source data table
   * @return reshaped table according to the format outlined above
   */
  def transposeColumnStatsIndex(spark: SparkSession, colStatsDF: DataFrame, queryColumns: Seq[String], tableSchema: StructType): DataFrame = {
    val colStatsSchema = colStatsDF.schema
    val colStatsSchemaOrdinalsMap = colStatsSchema.fields.zipWithIndex.map({
      case (field, ordinal) => (field.name, ordinal)
    }).toMap

    val tableSchemaFieldMap = tableSchema.fields.map(f => (f.name, f)).toMap

    val colNameOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME)
    val minValueOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE)
    val maxValueOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE)
    val fileNameOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
    val nullCountOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT)
    val valueCountOrdinal = colStatsSchemaOrdinalsMap(HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT)

    // NOTE: We have to collect list of indexed columns to make sure we properly align the rows
    //       w/in the transposed dataset: since some files might not have all of the columns indexed
    //       either due to the Column Stats Index config changes, schema evolution, etc, we have
    //       to make sure that all of the rows w/in transposed data-frame are properly padded (with null
    //       values) for such file-column combinations
    val indexedColumns: Seq[String] = colStatsDF.rdd.map(row => row.getString(colNameOrdinal)).distinct().collect()

    // NOTE: We're sorting the columns to make sure final index schema matches layout
    //       of the transposed table
    val sortedTargetColumns = TreeSet(queryColumns.intersect(indexedColumns): _*)

    val transposedRDD = colStatsDF.rdd
      .filter(row => sortedTargetColumns.contains(row.getString(colNameOrdinal)))
      .map { row =>
        if (row.isNullAt(minValueOrdinal) && row.isNullAt(maxValueOrdinal)) {
          // Corresponding row could be null in either of the 2 cases
          //    - Column contains only null values (in that case both min/max have to be nulls)
          //    - This is a stubbed Column Stats record (used as a tombstone)
          row
        } else {
          val minValueStruct = row.getAs[Row](minValueOrdinal)
          val maxValueStruct = row.getAs[Row](maxValueOrdinal)

          checkState(minValueStruct != null && maxValueStruct != null, "Invalid Column Stats record: either both min/max have to be null, or both have to be non-null")

          val colName = row.getString(colNameOrdinal)
          val colType = tableSchemaFieldMap(colName).dataType

          val (minValue, _) = tryUnpackNonNullVal(minValueStruct)
          val (maxValue, _) = tryUnpackNonNullVal(maxValueStruct)
          val rowValsSeq = row.toSeq.toArray
          // Update min-/max-value structs w/ unwrapped values in-place
          rowValsSeq(minValueOrdinal) = deserialize(minValue, colType)
          rowValsSeq(maxValueOrdinal) = deserialize(maxValue, colType)

          Row(rowValsSeq: _*)
        }
      }
      .groupBy(r => r.getString(fileNameOrdinal))
      .foldByKey(Seq[Row]()) {
        case (_, columnRowsSeq) =>
          // Rows seq is always non-empty (otherwise it won't be grouped into)
          val fileName = columnRowsSeq.head.get(fileNameOrdinal)
          val valueCount = columnRowsSeq.head.get(valueCountOrdinal)

          // To properly align individual rows (corresponding to a file) w/in the transposed projection, we need
          // to align existing column-stats for individual file with the list of expected ones for the
          // whole transposed projection (a superset of all files)
          val columnRowsMap = columnRowsSeq.map(row => (row.getString(colNameOrdinal), row)).toMap
          val alignedColumnRowsSeq = sortedTargetColumns.toSeq.map(columnRowsMap.get)

          val coalescedRowValuesSeq =
            alignedColumnRowsSeq.foldLeft(Seq[Any](fileName, valueCount)) {
              case (acc, opt) =>
                opt match {
                  case Some(columnStatsRow) =>
                    acc ++ Seq(minValueOrdinal, maxValueOrdinal, nullCountOrdinal).map(ord => columnStatsRow.get(ord))
                  case None =>
                    // NOTE: Since we're assuming missing column to essentially contain exclusively
                    //       null values, we set null-count to be equal to value-count (this behavior is
                    //       consistent with reading non-existent columns from Parquet)
                    acc ++ Seq(null, null, valueCount)
                }
            }

          Seq(Row(coalescedRowValuesSeq:_*))
      }
      .values
      .flatMap(it => it)

    // NOTE: It's crucial to maintain appropriate ordering of the columns
    //       matching table layout: hence, we cherry-pick individual columns
    //       instead of simply filtering in the ones we're interested in the schema
    val indexSchema = composeIndexSchema(sortedTargetColumns.toSeq, tableSchema)

    spark.createDataFrame(transposedRDD, indexSchema)
  }

  private def readFullColumnStatsIndexInternal(spark: SparkSession, metadataConfig: HoodieMetadataConfig, tableBasePath: String): DataFrame = {
    val metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(tableBasePath)
    // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]]
    spark.read.format("org.apache.hudi")
      .options(metadataConfig.getProps.asScala)
      .load(s"$metadataTablePath/${MetadataPartitionType.COLUMN_STATS.getPartitionPath}")
  }

  private def readColumnStatsIndexForColumnsInternal(spark: SparkSession, targetColumns: Seq[String], metadataConfig: HoodieMetadataConfig, tableBasePath: String) = {
    val ctx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))

    // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]] by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Deserializing fetched records into [[InternalRow]]s
    //    - Composing [[DataFrame]]
    val metadataTableDF = {
      val metadataTable = HoodieTableMetadata.create(ctx, metadataConfig, tableBasePath, FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue)

      // TODO encoding should be done internally w/in HoodieBackedTableMetadata
      val encodedTargetColumnNames = targetColumns.map(colName => new ColumnIndexID(colName).asBase64EncodedString())

      val recordsRDD: RDD[HoodieRecord[HoodieMetadataPayload]] =
        HoodieJavaRDD.getJavaRDD(
          metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames.asJava, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        )

      val catalystRowsRDD: RDD[InternalRow] = recordsRDD.mapPartitions { it =>
        val metadataRecordSchema = new Parser().parse(metadataRecordSchemaString)
        val converter = AvroConversionUtils.createAvroToInternalRowConverter(metadataRecordSchema, metadataRecordStructType)

        it.map { record =>
          // schema and props are ignored for generating metadata record from the payload
          // instead, the underlying file system, or bloom filter, or columns stats metadata (part of payload) are directly used
          toScalaOption(record.getData.getInsertValue(null, null))
            .flatMap(avroRecord => converter(avroRecord.asInstanceOf[GenericRecord]))
            .orNull
        }
      }

      HoodieUnsafeCatalystUtils.createDataFrame(spark, catalystRowsRDD, metadataRecordStructType)
    }
    metadataTableDF
  }
}

object ColumnStatsIndexSupport {

  private val metadataRecordSchemaString: String = HoodieMetadataRecord.SCHEMA$.toString
  private val metadataRecordStructType: StructType = AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataRecord.SCHEMA$)

  /**
   * @VisibleForTesting
   */
  def composeIndexSchema(targetColumnNames: Seq[String], tableSchema: StructType): StructType = {
    val fileNameField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME, StringType, nullable = true, Metadata.empty)
    val valueCountField = StructField(HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT, LongType, nullable = true, Metadata.empty)

    val targetFields = targetColumnNames.map(colName => tableSchema.fields.find(f => f.name == colName).get)

    StructType(
      targetFields.foldLeft(Seq(fileNameField, valueCountField)) {
        case (acc, field) =>
          acc ++ Seq(
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, field.dataType),
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, field.dataType),
            composeColumnStatStructType(field.name, HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, LongType))
      }
    )
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

  @inline private def composeColumnStatStructType(col: String, statName: String, dataType: DataType) =
    StructField(formatColName(col, statName), dataType, nullable = true, Metadata.empty)

  private def tryUnpackNonNullVal(statStruct: Row): (Any, Int) =
    statStruct.toSeq.zipWithIndex
      .find(_._1 != null)
      // NOTE: First non-null value will be a wrapper (converted into Row), bearing a single
      //       value
      .map { case (value, ord) => (value.asInstanceOf[Row].get(0), ord)}
      .getOrElse((null, -1))

  private def deserialize(value: Any, dataType: DataType): Any = {
    dataType match {
      // NOTE: Since we can't rely on Avro's "date", and "timestamp-micros" logical-types, we're
      //       manually encoding corresponding values as int and long w/in the Column Stats Index and
      //       here we have to decode those back into corresponding logical representation.
      case TimestampType => DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long])
      case DateType => DateTimeUtils.toJavaDate(value.asInstanceOf[Int])

      // NOTE: All integral types of size less than Int are encoded as Ints in MT
      case ShortType => value.asInstanceOf[Int].toShort
      case ByteType => value.asInstanceOf[Int].toByte

      case _ => value
    }
  }
}
