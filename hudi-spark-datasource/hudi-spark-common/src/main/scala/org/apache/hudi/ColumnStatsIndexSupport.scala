package org.apache.hudi

import org.apache.hudi.avro.model.HoodieMetadataColumnStats
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper.{getMaxColumnNameFor, getMinColumnNameFor, getNumNullsColumnNameFor}
import org.apache.hudi.metadata.{HoodieMetadataPayload, MetadataPartitionType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Mixin trait abstracting away heavy-lifting of interactions with Metadata Table's Column Stats Index,
 * providing convenient interfaces to read it, transpose, etc
 */
trait ColumnStatsIndexSupport {

  //
  //  |-- minValue: struct (nullable = true)
  //  |    |-- member0: struct (nullable = true)
  //  |    |    |-- value: <statistic_type> (nullable = false)
  //
  private val statisticRecordSchema: StructType =
    AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataColumnStats.SCHEMA$.getField("minValue").schema())

  private val statisticWrapperValueFieldName = "value"

  def readColumnStatsIndex(spark: SparkSession, metadataTablePath: String) = {
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
          case StructType(Array(valueField)) => valueField.dataType == colType
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
            .withColumnRenamed(HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, getNumNullsColumnNameFor(colName))
            .withColumn(
              getMinColumnNameFor(colName),
              composeStatCoercionExpr(HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, carryingField, colType))
            .withColumn(
              getMaxColumnNameFor(colName),
              composeStatCoercionExpr(HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, carryingField, colType))
            .drop(
              HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME,
              HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
              HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE)
      }
      .reduceLeft((left, right) =>
        left.join(right, usingColumn = HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME))

    transposedColStatsDF
  }

  private def composeStatCoercionExpr(statColumnName: String, carryingField: StructField, tableColType: DataType) = {
    col(s"$statColumnName.${carryingField.name}.$statisticWrapperValueFieldName").cast(tableColType)
  }
}
