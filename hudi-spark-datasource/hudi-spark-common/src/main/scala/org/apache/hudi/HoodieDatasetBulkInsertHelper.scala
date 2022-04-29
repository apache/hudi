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

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.util.ReflectionUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.BuiltinKeyGenerator
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieUnsafeRDDUtils.createDataFrame
import org.apache.spark.sql.HoodieUnsafeRowUtils.{composeNestedFieldPath, getNestedRowValue}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, HoodieUnsafeRDDUtils, Row}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.asScalaBufferConverter

object HoodieDatasetBulkInsertHelper extends Logging {

  /**
   * Prepares [[DataFrame]] for bulk-insert into Hudi table, taking following steps:
   *
   * <ol>
   *   <li>Invoking configured [[KeyGenerator]] to produce record key, alas partition-path value</li>
   *   <li>Prepends Hudi meta-fields to every row in the dataset</li>
   *   <li>Dedupes rows (if necessary)</li>
   *   <li>Partitions dataset using provided [[partitioner]]</li>
   * </ol>
   */
  def prepareForBulkInsert(df: DataFrame,
                           config: HoodieWriteConfig,
                           partitioner: BulkInsertPartitioner[Dataset[Row]],
                           isGlobalIndex: Boolean,
                           dropPartitionColumns: Boolean): Dataset[Row] = {
    val populateMetaFields = config.populateMetaFields()
    val schema = df.schema

    val keyGeneratorClassName = config.getStringOrThrow(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME,
      "Key-generator class name is required")

    val prependedRdd: RDD[InternalRow] =
      df.queryExecution.toRdd.mapPartitions { iter =>
        lazy val keyGenerator =
          ReflectionUtils.loadClass(keyGeneratorClassName, new TypedProperties(config.getProps))
            .asInstanceOf[BuiltinKeyGenerator]

        iter.map { row =>
          val (recordKey, partitionPath) =
            if (populateMetaFields) {
              (UTF8String.fromString(keyGenerator.getRecordKey(row, schema)),
                UTF8String.fromString(keyGenerator.getPartitionPath(row, schema)))
            } else {
              (UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8)
            }
          val commitTimestamp = UTF8String.EMPTY_UTF8
          val commitSeqNo = UTF8String.EMPTY_UTF8
          val filename = UTF8String.EMPTY_UTF8
          // To minimize # of allocations, we're going to allocate a single array
          // setting all column values in place for the updated row
          val newColVals = new Array[Any](schema.fields.length + HoodieRecord.HOODIE_META_COLUMNS.size)
          // NOTE: Order of the fields have to match that one of `HoodieRecord.HOODIE_META_COLUMNS`
          newColVals.update(0, commitTimestamp)
          newColVals.update(1, commitSeqNo)
          newColVals.update(2, recordKey)
          newColVals.update(3, partitionPath)
          newColVals.update(4, filename)
          // Prepend existing row column values
          row.toSeq(schema).copyToArray(newColVals, 5)
          new GenericInternalRow(newColVals)
        }
      }

    val metaFields = Seq(
      StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType),
      StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, StringType),
      StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType),
      StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, StringType),
      StructField(HoodieRecord.FILENAME_METADATA_FIELD, StringType))

    val updatedSchema = StructType(metaFields ++ schema.fields)
    val updatedDF = HoodieUnsafeRDDUtils.createDataFrame(df.sparkSession, prependedRdd, updatedSchema)

    if (!populateMetaFields) {
      updatedDF
    } else {
      val trimmedDF = if (dropPartitionColumns) {
        val keyGenerator = ReflectionUtils.loadClass(keyGeneratorClassName, new TypedProperties(config.getProps)).asInstanceOf[BuiltinKeyGenerator]
        val partitionPathFields = keyGenerator.getPartitionPathFields.asScala
        val nestedPartitionPathFields = partitionPathFields.filter(f => f.contains('.'))
        if (nestedPartitionPathFields.nonEmpty) {
          logWarning(s"Can not drop nested partition path fields: $nestedPartitionPathFields")
        }

        val partitionPathCols = partitionPathFields -- nestedPartitionPathFields
        updatedDF.drop(partitionPathCols: _*)
      } else {
        updatedDF
      }

      val dedupedDF = if (config.shouldCombineBeforeInsert) {
        dedupeRows(trimmedDF, config.getPreCombineField, isGlobalIndex)
      } else {
        trimmedDF
      }

      partitioner.repartitionRecords(dedupedDF, config.getBulkInsertShuffleParallelism)
    }
  }

  private def dedupeRows(df: DataFrame, preCombineFieldRef: String, isGlobalIndex: Boolean): DataFrame = {
    val recordKeyMetaFieldOrd = df.schema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)
    val partitionPathMetaFieldOrd = df.schema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
    // NOTE: Pre-combine field could be a nested field
    val preCombineFieldPath = composeNestedFieldPath(df.schema, preCombineFieldRef)

    val dedupedRdd = df.queryExecution.toRdd
      .map { row =>
        val rowKey = if (isGlobalIndex) {
          row.getString(recordKeyMetaFieldOrd)
        } else {
          val partitionPath = row.getString(partitionPathMetaFieldOrd)
          val recordKey = row.getString(recordKeyMetaFieldOrd)
          s"$partitionPath:$recordKey"
        }
        // NOTE: It's critical whenever we keep the reference to the row, to make a copy
        //       since Spark might be providing us with a mutable copy (updated during the iteration)
        (rowKey, row.copy())
      }
      .reduceByKey {
        (oneRow, otherRow) =>
          val onePreCombineVal = getNestedRowValue(oneRow, preCombineFieldPath).asInstanceOf[Ordered[AnyRef]]
          val otherPreCombineVal = getNestedRowValue(otherRow, preCombineFieldPath).asInstanceOf[Ordered[AnyRef]]
          if (onePreCombineVal.compareTo(otherPreCombineVal.asInstanceOf[AnyRef]) >= 0) {
            oneRow
          } else {
            otherRow
          }
      }
      .map {
        case (_, row) => row
      }

    createDataFrame(df.sparkSession, dedupedRdd, df.schema)
  }
}
