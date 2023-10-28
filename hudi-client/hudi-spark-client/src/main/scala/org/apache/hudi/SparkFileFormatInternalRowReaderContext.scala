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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.io.storage.HoodieSparkParquetReader
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.spark.TaskContext
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFooterReader, ParquetOptions, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * Implementation of {@link HoodieReaderContext} to read {@link InternalRow}s with
 * {@link ParquetFileFormat} on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param baseFileReader  A reader that transforms a {@link PartitionedFile} to an iterator of {@link InternalRow}.
 * @param partitionSchema The partition schema in {@link StructType}.
 * @param partitionValues The values for a partition in which the file group lives.
 */
class SparkFileFormatInternalRowReaderContext(baseFileReader: PartitionedFile => Iterator[InternalRow],
                                              partitionSchema: StructType,
                                              partitionValues: InternalRow) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter = SparkAdapterSupport.sparkAdapter
  val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()

  override def getFileRecordIterator(filePath: Path,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     conf: Configuration): ClosableIterator[InternalRow] = {
    if (FSUtils.isLogFile(filePath)) {
      val sqlConf = SQLConf.get
      if (sqlConf.parquetVectorizedReaderEnabled) {
        val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
        val capacity = sqlConf.parquetVectorizedReaderBatchSize
        val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
        val parquetOptions = new ParquetOptions(Map[String, String](), sqlConf)
        val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
        val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

        lazy val footerFileMetaData =
          ParquetFooterReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData

        // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
        // *only* if the file was created by something other than "parquet-mr", so check the actual
        // writer here for this file.  We have to do this per-file, as each file in the table may
        // have different writers.
        // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
        def isCreatedByParquetMr: Boolean =
          footerFileMetaData.getCreatedBy.startsWith("parquet-mr")

        val convertTz =
          if (timestampConversion && !isCreatedByParquetMr) {
            Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
          } else {
            None
          }
        val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          int96RebaseModeInRead)
        val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          datetimeRebaseModeInRead)
        val taskContext = Option(TaskContext.get())

        val vectorizedReader = new VectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseSpec.mode.toString,
          datetimeRebaseSpec.timeZone,
          int96RebaseSpec.mode.toString,
          int96RebaseSpec.timeZone,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)

        val iter = new RecordReaderIterator(vectorizedReader)
        try {
          vectorizedReader.initialize(filePath.toUri.toString, null)
          //vectorizedReader.initBatch(partitionSchema, partitionValues)
          //vectorizedReader.enableReturningBatches()
          new CloseableInternalRowIterator(iter.asInstanceOf[Iterator[InternalRow]])
        } catch {
          case e: Throwable =>
            // In case there is an exception in initialization, close the iterator to
            // avoid leaking resources.
            iter.close()
            throw e
        }
      } else {
        new HoodieSparkParquetReader(conf, filePath).getInternalRowIterator(dataSchema, dataSchema)
      }
    } else {
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(partitionValues, filePath, start, length)
      new CloseableInternalRowIterator(baseFileReader.apply(fileInfo))
    }
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an engine-specific record.
   *
   * @param avroRecord The Avro record.
   * @return An engine-specific record in Type {@link T}.
   */
  override def convertAvroRecord(avroRecord: IndexedRecord): InternalRow = {
    val schema = avroRecord.getSchema
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val deserializer = deserializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroDeserializer(schema, structType)
    })
    deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
  }
}
