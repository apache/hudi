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
import org.apache.hadoop.fs.Path
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class Spark35OrcReader(enableVectorizedReader: Boolean,
                       memoryMode: MemoryMode,
                       dataSchema: StructType,
                       orcFilterPushDown: Boolean,
                       isCaseSensitive: Boolean,
                       capacity: Int) extends SparkOrcReaderBase(enableVectorizedReader, dataSchema, orcFilterPushDown, isCaseSensitive) {

  override def partitionedFileToPath(file: PartitionedFile): Path = {
    file.toPath
  }

  override def buildReader(): OrcColumnarBatchReader = {
    new OrcColumnarBatchReader(capacity, memoryMode)
  }

  override def structTypeToAttributes(schema: StructType): Seq[Attribute] = {
    toAttributes(schema)
  }
}

object Spark35OrcReader {
  /**
   * Get ORC file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return ORC file reader
   */
  def build(vectorized: Boolean,
            sqlConf: SQLConf,
            options: Map[String, String],
            hadoopConf: Configuration,
            dataSchema: StructType): Spark35OrcReader = {
    //set hadoopconf
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)

    val memoryMode = if (sqlConf.offHeapColumnVectorEnabled) {
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }

    val enableVectorizedReader = sqlConf.orcVectorizedReaderEnabled &&
      options.getOrElse(FileFormat.OPTION_RETURNING_BATCH,
          throw new IllegalArgumentException(
            "OPTION_RETURNING_BATCH should always be set for OrcFileFormat. " +
              "To workaround this issue, set spark.sql.orc.enableVectorizedReader=false."))
        .equals("true")

    new Spark35OrcReader(
      enableVectorizedReader = enableVectorizedReader && vectorized,
      memoryMode = memoryMode,
      isCaseSensitive = sqlConf.caseSensitiveAnalysis,
      capacity = sqlConf.orcVectorizedReaderBatchSize,
      orcFilterPushDown = sqlConf.orcFilterPushDown,
      dataSchema = dataSchema)
  }
}
