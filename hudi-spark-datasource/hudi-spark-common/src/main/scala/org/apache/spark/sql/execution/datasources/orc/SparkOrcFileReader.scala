/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.orc.{OrcFile, Reader, TypeDescription}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, SparkFileReader, SparkFileReaderBase, SparkParquetReaderBuilder}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter}

class SparkOrcFileReader(enableVectorizedReader: Boolean,
                           datetimeRebaseModeInRead: String,
                           int96RebaseModeInRead: String,
                           enableParquetFilterPushDown: Boolean,
                           pushDownDate: Boolean,
                           pushDownTimestamp: Boolean,
                           pushDownDecimal: Boolean,
                           pushDownInFilterThreshold: Int,
                           pushDownStringPredicate: Boolean,
                           isCaseSensitive: Boolean,
                           timestampConversion: Boolean,
                           enableOffHeapColumnVector: Boolean,
                           capacity: Int,
                           returningBatch: Boolean,
                           enableRecordFilter: Boolean,
                           timeZoneId: Option[String]) extends SparkFileReaderBase(
  enableVectorizedReader = enableVectorizedReader,
  enableParquetFilterPushDown = enableParquetFilterPushDown,
  pushDownDate = pushDownDate,
  pushDownTimestamp = pushDownTimestamp,
  pushDownDecimal = pushDownDecimal,
  pushDownInFilterThreshold = pushDownInFilterThreshold,
  isCaseSensitive = isCaseSensitive,
  timestampConversion = timestampConversion,
  enableOffHeapColumnVector = enableOffHeapColumnVector,
  capacity = capacity,
  returningBatch = returningBatch,
  enableRecordFilter = enableRecordFilter,
  timeZoneId = timeZoneId) {
  /**
   * Implemented for each spark version
   *
   * @param file              parquet file to read
   * @param requiredSchema    desired output schema of the data
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param sharedConf        the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override protected def doRead(file: PartitionedFile,
                                requiredSchema: StructType,
                                partitionSchema: StructType,
                                internalSchemaOpt: util.Option[InternalSchema],
                                filters: Seq[Filter],
                                sharedConf: Configuration): Iterator[InternalRow] = {
    val path = file.filePath.toPath
    val reader: Reader = OrcFile.createReader(path, OrcFile.readerOptions(sharedConf))

    // Create the row batch
    val schema = reader.getSchema
    val options = reader.options()
    val rows = reader.rows(options)
    val batch = schema.createRowBatch()

    // ColumnarBatch to hold rows
    val sparkBatch = new ColumnarBatch(
      schema.getChildren.asScala.map { field =>
        new OnHeapColumnVector(batch.size, orcTypeToSparkType(field))
      }.toArray
    )

    new Iterator[InternalRow] {
      private var hasMoreRows = rows.nextBatch(batch)

      override def hasNext: Boolean = hasMoreRows

      override def next(): InternalRow = {
        if (hasMoreRows) {
          val row = sparkBatch.rowIterator.asScala.next()
          if (!sparkBatch.rowIterator.asScala.hasNext) {
            hasMoreRows = rows.nextBatch(batch)
          }
          row
        } else {
          throw new NoSuchElementException("No more rows available.")
        }
      }
    }
  }

  def orcTypeToSparkType(orcType: TypeDescription): DataType = {
    orcType.getCategory match {
      case TypeDescription.Category.BOOLEAN => BooleanType
      case TypeDescription.Category.BYTE => ByteType
      case TypeDescription.Category.SHORT => ShortType
      case TypeDescription.Category.INT => IntegerType
      case TypeDescription.Category.LONG => LongType
      case TypeDescription.Category.FLOAT => FloatType
      case TypeDescription.Category.DOUBLE => DoubleType
      case TypeDescription.Category.STRING => StringType
      case TypeDescription.Category.CHAR => StringType
      case TypeDescription.Category.VARCHAR => StringType
      case TypeDescription.Category.DATE => DateType
      case TypeDescription.Category.TIMESTAMP => TimestampType
      case TypeDescription.Category.DECIMAL => DecimalType(orcType.getPrecision, orcType.getScale)
      case TypeDescription.Category.BINARY => BinaryType
      case TypeDescription.Category.LIST =>
        ArrayType(orcTypeToSparkType(orcType.getChildren.get(0)))
      case TypeDescription.Category.MAP =>
        MapType(
          orcTypeToSparkType(orcType.getChildren.get(0)),
          orcTypeToSparkType(orcType.getChildren.get(1))
        )
      case TypeDescription.Category.STRUCT =>
        StructType(
          orcType.getFieldNames.asScala.zip(orcType.getChildren.asScala).map {
            case (name, childType) => StructField(name, orcTypeToSparkType(childType))
          }
        )
      case _ =>
        throw new IllegalArgumentException(s"Unsupported ORC type: ${orcType.getCategory}")
    }
  }
}

object SparkOrcFileReader extends SparkParquetReaderBuilder {
  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return parquet file reader
   */
  def build(vectorized: Boolean,
            sqlConf: SQLConf,
            options: Map[String, String],
            hadoopConf: Configuration): SparkFileReader = {
    //set hadoopconf
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, sqlConf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, sqlConf.isParquetINT96AsTimestamp)
    // Using string value of this conf to preserve compatibility across spark versions. See [HUDI-5868]
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sqlConf.getConfString(
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.defaultValueString).toBoolean
    )
    hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, sqlConf.parquetInferTimestampNTZEnabled)

    val returningBatch = sqlConf.parquetVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
          throw new IllegalArgumentException(
            "OPTION_RETURNING_BATCH should always be set for ParquetFileFormat. " +
              "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false."))
        .equals("true")

    val parquetOptions = new ParquetOptions(options, sqlConf)
    new SparkOrcFileReader(
      enableVectorizedReader = vectorized,
      datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead,
      int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead,
      enableParquetFilterPushDown = sqlConf.parquetFilterPushDown,
      pushDownDate = sqlConf.parquetFilterPushDownDate,
      pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp,
      pushDownDecimal = sqlConf.parquetFilterPushDownDecimal,
      pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold,
      pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate,
      isCaseSensitive = sqlConf.caseSensitiveAnalysis,
      timestampConversion = sqlConf.isParquetINT96TimestampConversion,
      enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled,
      capacity = sqlConf.parquetVectorizedReaderBatchSize,
      returningBatch = returningBatch,
      enableRecordFilter = sqlConf.parquetRecordFilterEnabled,
      timeZoneId = Some(sqlConf.sessionLocalTimeZone))
  }
}
