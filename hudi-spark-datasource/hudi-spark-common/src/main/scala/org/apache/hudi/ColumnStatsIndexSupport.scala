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

import org.apache.hudi.ColumnStatsIndexSupport.{genStatValueExtractionExpr, statisticRecordSchema}
import org.apache.hudi.avro.model.HoodieMetadataColumnStats
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper.{getMaxColumnNameFor, getMinColumnNameFor, getNumNullsColumnNameFor}
import org.apache.hudi.metadata.{HoodieMetadataPayload, MetadataPartitionType}
import org.apache.spark.sql.HoodieSparkTypeUtils.isWiderThan
import org.apache.spark.sql.HoodieUnsafeRDDUtils.createDataFrame
import org.apache.spark.sql.{DataFrame, Encoders, HoodieUnsafeRDDUtils, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, ByteType, DataType, DecimalType, IntegerType, ShortType, StructField, StructType}

/**
 * Mixin trait abstracting away heavy-lifting of interactions with Metadata Table's Column Stats Index,
 * providing convenient interfaces to read it, transpose, etc
 */
trait ColumnStatsIndexSupport {

  def readColumnStatsIndex(spark: SparkSession, metadataTablePath: String): DataFrame = {
    val targetColStatsIndexColumns = Seq(
      HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
      HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT)

    val requiredMetadataIndexColumns =
      (targetColStatsIndexColumns :+ HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME).map(colName =>
        s"${HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS}.${colName}")

    // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]]
    val metadataTableDF = spark.read.format("org.apache.hudi")
      .load(s"$metadataTablePath/${MetadataPartitionType.COLUMN_STATS.getPartitionPath}")

    // TODO filter on (column, partition) prefix
    val colStatsDF = metadataTableDF.where(col(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).isNotNull)
      .select(requiredMetadataIndexColumns.map(col): _*)

    colStatsDF
  }

  private def findCarryingField(colType: DataType, statisticStructType: StructType): StructField = {
    statisticStructType.fields
      .find { wrapperField =>
        wrapperField.dataType match {
          case StructType(Array(valueField)) =>
            (colType, valueField.dataType) match {
              // NOTE: Due to misalignment type-systems of Parquet, Spark and Avro we have to do up-casts
              //       while persisting records w/in Column Stats Index: for ex, if in Parquet value is stored as
              //       [[Short]] (2 bytes) inside the Index it will be persisted as [[Integer]], since Avro's type-system
              //       simply does not have any 2-byte integral type. Therefore when we deserialize we have to
              //       do down-cast back into the typ of the Column the data belongs to, therefore such casts could not
              //       lead to the data loss.
              case (colDecimalType: DecimalType, valueDecimalType: DecimalType) => isWiderThan(valueDecimalType, colDecimalType)
              case (ShortType, IntegerType) => true
              case (ByteType, IntegerType) => true

              case (_: StructType, BinaryType) => true

              case (colType, valueType) => valueType == colType
            }

          case _ =>
            throw new IllegalArgumentException(s"Invalid format of the statistic wrapper (${wrapperField.dataType})")
        }
      }
      .getOrElse(throw new IllegalArgumentException(s"Unsupported primitive type for the statistic ($colType)"))
  }

  def transposeColumnStatsIndex(columnStatsDF: DataFrame, targetDataTableColumns: Seq[(String, DataType)]): DataFrame = {
    //
    // Metadata Table bears rows in the following format
    //
    //  +---------------------------+------------+------------+------------+-------------+
    //  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
    //  +---------------------------+------------+------------+------------+-------------+
    //  | one_base_file.parquet     |          A |          1 |         10 |           0 |
    //  | another_base_file.parquet |          A |        -10 |          0 |           5 |
    //  +---------------------------+------------+------------+------------+-------------+
    //
    // While Data Skipping utils are expecting following (transposed) format, where per-column stats are
    // essentially transposed (from rows to columns):
    //
    //  +---------------------------+------------+------------+-------------+
    //  |          file             | A_minValue | A_maxValue | A_num_nulls |
    //  +---------------------------+------------+------------+-------------+
    //  | one_base_file.parquet     |          1 |         10 |           0 |
    //  | another_base_file.parquet |        -10 |          0 |           5 |
    //  +---------------------------+------------+------------+-------------+
    //
    // NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
    //       query at hand might only be referencing a handful of those. As such, we collect all the
    //       column references from the filtering expressions, and only transpose records corresponding to the
    //       columns referenced in those
    //
    val transposedColStatsDF = targetDataTableColumns
      .map {
        case (colName, colType) =>
          val carryingField = findCarryingField(colType, statisticRecordSchema)

          columnStatsDF.filter(col(HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME).equalTo(colName))
            .withColumn(
              getMinColumnNameFor(colName),
              genStatValueExtractionExpr(HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, carryingField, colType)
            )
            .withColumn(
              getMaxColumnNameFor(colName),
              genStatValueExtractionExpr(HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, carryingField, colType)
            )
            .withColumnRenamed(HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, getNumNullsColumnNameFor(colName))
            .drop(
              HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME,
              HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
              HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE)
      }
      .reduceLeft((left, right) =>
        left.join(right, usingColumn = HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME))

    transposedColStatsDF
  }
}

object ColumnStatsIndexSupport {

  //
  //  |-- minValue: struct (nullable = true)
  //  |    |-- member0: struct (nullable = true)
  //  |    |    |-- value: <statistic_type> (nullable = false)
  //
  private val statisticRecordSchema: StructType =
  AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataColumnStats.SCHEMA$.getField("minValue").schema())

  private val statisticWrapperValueFieldName = "value"

  private def genStatValueExtractionExpr(statColumnName: String, carryingField: StructField, tableColType: DataType) = {
    col(s"$statColumnName.${carryingField.name}.$statisticWrapperValueFieldName").cast(tableColType)
  }

//  private def genSelectNonNullValueExpr(statColumnName: String, statisticRecordSchema: StructType): Column = {
//    statisticRecordSchema.fields
//      .foldLeft[Column](null) {
//        case (acc, wrapperField) =>
//          if (acc == null)
//            when(
//              col(s"$statColumnName.${wrapperField.name}").isNotNull,
//              col(s"$statColumnName.${wrapperField.name}.$statisticWrapperValueFieldName"))
//          else
//            acc.when(
//              col(s"$statColumnName.${wrapperField.name}").isNotNull,
//              col(s"$statColumnName.${wrapperField.name}.$statisticWrapperValueFieldName")
//            )
//      }
//  }

}
