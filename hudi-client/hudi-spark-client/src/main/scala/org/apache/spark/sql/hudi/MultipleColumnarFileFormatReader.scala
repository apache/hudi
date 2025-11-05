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

package org.apache.spark.sql.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class MultipleColumnarFileFormatReader(parquetReader: SparkColumnarFileReader, orcReader: SparkColumnarFileReader)
  extends SparkColumnarFileReader with SparkAdapterSupport {

  /**
   * Read an individual file
   *
   * @param file              file to read
   * @param requiredSchema    desired output schema of the data
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @param tableSchemaOpt    option of table schema for timestamp precision conversion fix.
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile, requiredSchema: StructType, partitionSchema: StructType, internalSchemaOpt: util.Option[InternalSchema], filters: Seq[Filter],
                    storageConf: StorageConfiguration[Configuration], tableSchemaOpt: util.Option[org.apache.parquet.schema.MessageType]): Iterator[InternalRow] = {
    val filePath = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
    val fileFormat = HoodieFileFormat.fromFileExtension(filePath.getFileExtension)
    fileFormat match {
      case HoodieFileFormat.PARQUET =>
        parquetReader.read(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaOpt)
      case HoodieFileFormat.ORC =>
        orcReader.read(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, storageConf, tableSchemaOpt)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format for file: $filePath")
    }
  }
}
