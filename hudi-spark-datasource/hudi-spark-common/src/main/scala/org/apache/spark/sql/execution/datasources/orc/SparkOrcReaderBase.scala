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

import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator, SparkColumnarFileReader}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


abstract class SparkOrcReaderBase(enableVectorizedReader: Boolean,
                                  dataSchema: StructType,
                                  orcFilterPushDown: Boolean,
                                  isCaseSensitive: Boolean) extends SparkColumnarFileReader  {
  /**
   * Read an individual ORC file
   *
   * @param file              ORC file to read
   * @param requiredSchema    desired output schema of the data
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile, requiredSchema: StructType, partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema], filters: Seq[Filter],
                    storageConf: StorageConfiguration[Configuration], tableSchemaOpt: util.Option[org.apache.parquet.schema.MessageType]): Iterator[InternalRow] = {
    val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val conf = storageConf.unwrap()

    val filePath = partitionedFileToPath(file)

    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val orcSchema =
      Utils.tryWithResource(OrcFile.createReader(filePath, readerOptions))(_.getSchema)
    val resultedColPruneInfo = OrcUtils.requestedColumnIds(
      isCaseSensitive, dataSchema, requiredSchema, orcSchema, conf)

    if (resultedColPruneInfo.isEmpty) {
      Iterator.empty
    } else {
      // ORC predicate pushdown
      if (orcFilterPushDown && filters.nonEmpty) {
        val fileSchema = OrcUtils.toCatalystSchema(orcSchema)
        OrcFilters.createFilter(fileSchema, filters).foreach { f =>
          OrcInputFormat.setSearchArgument(conf, f, fileSchema.fieldNames)
        }
      }

      val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
      val resultSchemaString = OrcUtils.orcResultSchemaString(canPruneCols,
        dataSchema, resultSchema, partitionSchema, conf)
      assert(requestedColIds.length == requiredSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskConf = new Configuration(conf)

      val includeColumns = requestedColIds.filter(_ != -1).sorted.mkString(",")
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, includeColumns)
      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      if (enableVectorizedReader) {
        val batchReader = buildReader()
        // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
        // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
        // after opening a file.
        val iter = new RecordReaderIterator(batchReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        val requestedDataColIds = requestedColIds ++ Array.fill(partitionSchema.length)(-1)
        val requestedPartitionColIds =
          Array.fill(requiredSchema.length)(-1) ++ Range(0, partitionSchema.length)
        batchReader.initialize(fileSplit, taskAttemptContext)
        batchReader.initBatch(
          TypeDescription.fromString(resultSchemaString),
          resultSchema.fields,
          requestedDataColIds,
          requestedPartitionColIds,
          file.partitionValues)

        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        val orcRecordReader = new OrcInputFormat[OrcStruct]
          .createRecordReader(fileSplit, taskAttemptContext)
        val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

        val fullSchema = structTypeToAttributes(requiredSchema) ++ structTypeToAttributes(partitionSchema)
        val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
        val deserializer = new OrcDeserializer(requiredSchema, requestedColIds)

        if (partitionSchema.length == 0) {
          iter.map(value => unsafeProjection(deserializer.deserialize(value)))
        } else {
          val joinedRow = new JoinedRow()
          iter.map(value =>
            unsafeProjection(joinedRow(deserializer.deserialize(value), file.partitionValues)))
        }
      }
    }
  }

  def partitionedFileToPath(file: PartitionedFile): Path

  def buildReader(): OrcColumnarBatchReader

  def structTypeToAttributes(schema: StructType): Seq[Attribute]
}
