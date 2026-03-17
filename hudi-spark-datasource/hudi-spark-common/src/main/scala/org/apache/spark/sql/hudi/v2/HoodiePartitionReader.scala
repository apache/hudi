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

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

/**
 * Partition reader that reads rows from a single CoW base file using [[SparkColumnarFileReader]].
 */
class

HoodiePartitionReader(partition: HoodieInputPartition,
                            broadcastReader: Broadcast[SparkColumnarFileReader],
                            broadcastConf: Broadcast[SerializableConfiguration],
                            readSchema: StructType,
                            requiredDataSchema: StructType,
                            requiredPartitionSchema: StructType)
  extends PartitionReader[InternalRow] with SparkAdapterSupport {

  private var rawIterator: Iterator[InternalRow] = _
  private val iter: Iterator[InternalRow] = createIterator()
  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      current = iter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    rawIterator match {
      case c: Closeable => c.close()
      case _ =>
    }
  }

  private def createIterator(): Iterator[InternalRow] = {
    val partValues = InternalRow.fromSeq(partition.partitionValues.toSeq)

    val pFile = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(partValues, new StoragePath(partition.baseFilePath), 0L, partition.baseFileLength)

    val storageConf = new HadoopStorageConfiguration(broadcastConf.value.value)
    rawIterator = broadcastReader.value.read(
      pFile, requiredDataSchema, requiredPartitionSchema,
      HOption.empty(), Seq.empty, storageConf)

    val readerOutputSchema = StructType(requiredDataSchema.fields ++ requiredPartitionSchema.fields)
    if (readerOutputSchema != readSchema) {
      val projection = HoodieCatalystExpressionUtils.generateUnsafeProjection(readerOutputSchema, readSchema)
      rawIterator.map(row => projection(row))
    } else {
      rawIterator
    }
  }
}
