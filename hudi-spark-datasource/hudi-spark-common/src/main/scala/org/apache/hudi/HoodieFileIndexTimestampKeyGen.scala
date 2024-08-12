/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.exception.HoodieValidationException
import org.apache.hudi.keygen.CustomAvroKeyGenerator
import org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.types.StructType

class HoodieFileIndexTimestampKeyGen(override val spark: SparkSession,
                                          override val metaClient: HoodieTableMetaClient,
                                          override val schemaSpec: Option[StructType],
                                          override val options: Map[String, String],
                                          @transient override val fileStatusCache: FileStatusCache = NoopCache,
                                          override val includeLogFiles: Boolean = false,
                                          override val shouldEmbedFileSlices: Boolean = false)
  extends HoodieFileIndex(
    spark = spark,
    metaClient = metaClient,
    schemaSpec = schemaSpec,
    options = options,
    fileStatusCache = fileStatusCache,
    includeLogFiles = includeLogFiles,
    shouldEmbedFileSlices = shouldEmbedFileSlices) {

  def getTimestampPartitionIndex(): Set[Int] = {
    val tableConfig = metaClient.getTableConfig
    val keyGeneratorClassName = tableConfig.getKeyGeneratorClassName
    if (keyGeneratorClassName.equals(KeyGeneratorType.TIMESTAMP.getClassName)
      || keyGeneratorClassName.equals(KeyGeneratorType.TIMESTAMP_AVRO.getClassName)) {
      Set(0)
    } else if (keyGeneratorClassName.equals(KeyGeneratorType.CUSTOM.getClassName)
      || keyGeneratorClassName.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName)) {
      val partitionFields = HoodieTableConfig.getPartitionFieldsForKeyGenerator(tableConfig).orElse(java.util.Collections.emptyList[String]())
      val partitionTypes = CustomAvroKeyGenerator.getPartitionTypes(partitionFields)
      var partitionIndexes: Set[Int] = Set.empty
      for (i <- 0 until partitionTypes.size()) {
        if (partitionTypes.get(i).equals(PartitionKeyType.TIMESTAMP)) {
          partitionIndexes = partitionIndexes + i
        }
      }
      partitionIndexes
    } else {
      throw new HoodieValidationException("Expected timestamp or custom keygen with HoodieFileIndexTimestampKeyGen")
    }
  }

  /**
   * Invoked by Spark to fetch list of latest base files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters      data columns filters
   * @return list of PartitionDirectory containing partition to base files mapping
   */
  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitionDirectories = super.listFiles(partitionFilters, dataFilters)

    val timestampPartitionIndexes = getTimestampPartitionIndex()
    partitionDirectories.map(dir =>
      dir.values match {
        case mapping: HoodiePartitionFileSliceMapping =>
          val oldValues = mapping.values
          val finalValues = InternalRow.fromSeq(oldValues.toSeq(_partitionSchemaFromProperties)
            .zipWithIndex.map { case (elem, index) => convertTimestampPartitionType(timestampPartitionIndexes, index, elem) })
          dir.copy(values = new HoodiePartitionFileSliceMapping(finalValues, mapping.getFileSlices()))
        case _ =>
          val newValues = dir.values.toSeq(_partitionSchemaFromProperties)
            .zipWithIndex.map { case (elem, index) => convertTimestampPartitionType(timestampPartitionIndexes, index, elem) }
          dir.copy(values = InternalRow.fromSeq(newValues))
      }
    )
  }

  private def convertTimestampPartitionType(timestampPartitionIndexes: Set[Int], index: Int, elem: Any) = {
    if (timestampPartitionIndexes.contains(index)) org.apache.spark.unsafe.types.UTF8String.fromString(String.valueOf(elem)) else elem
  }

  override def getPartitionSchema(): StructType = {
    sparkAdapter.getSparkParsePartitionUtil.getPartitionSchema(metaClient.getTableConfig, schema, handleCustomKeyGenerator = true)
  }
}
