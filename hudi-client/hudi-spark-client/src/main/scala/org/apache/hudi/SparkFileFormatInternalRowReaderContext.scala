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
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{HoodieInternalRowUtils, SparkSession}

import scala.collection.mutable

/**
 * Implementation of {@link HoodieReaderContext} to read {@link InternalRow}s with
 * {@link ParquetFileFormat} on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param sparkSession    Spark session.
 * @param baseFileReader  A reader that transforms a {@link PartitionedFile} to an iterator of {@link InternalRow}.
 * @param partitionSchema The partition schema in {@link StructType}.
 * @param partitionValues The values for a partition in which the file group lives.
 */
class SparkFileFormatInternalRowReaderContext(@transient sparkSession: SparkSession,
                                              baseFileReader: PartitionedFile => Iterator[InternalRow],
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
    val fileInfo = sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(partitionValues, filePath, start, length)
    if (filePath.toString.contains(HoodieLogFile.DELTA_EXTENSION)) {
      val dataStructType = HoodieInternalRowUtils.getCachedSchema(dataSchema)
      new CloseableInternalRowIterator(new ParquetFileFormat().buildReaderWithPartitionValues(
        sparkSession, dataStructType, partitionSchema, dataStructType, Seq.empty, Map.empty, conf
      ).apply(fileInfo))
    } else {
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
