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

import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.util.ReflectionUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.index.SparkHoodieIndexFactory
import org.apache.hudi.keygen.{BuiltinKeyGenerator, SparkKeyGeneratorInterface}
import org.apache.hudi.table.BulkInsertPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieUnsafeRowUtils.{composeNestedFieldPath, getNestedInternalRowValue}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, HoodieUnsafeUtils, Row}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

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
                           shouldDropPartitionColumns: Boolean): Dataset[Row] = {
    val populateMetaFields = config.populateMetaFields()
    val schema = df.schema

    val keyGeneratorClassName = config.getStringOrThrow(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME,
      "Key-generator class name is required")

    val prependedRdd: RDD[InternalRow] =
      df.queryExecution.toRdd.mapPartitions { iter =>
        val keyGenerator =
          ReflectionUtils.loadClass(keyGeneratorClassName, new TypedProperties(config.getProps))
            .asInstanceOf[SparkKeyGeneratorInterface]

        iter.map { row =>
          val (recordKey, partitionPath) =
            if (populateMetaFields) {
              (keyGenerator.getRecordKey(row, schema), keyGenerator.getPartitionPath(row, schema))
            } else {
              (UTF8String.EMPTY_UTF8, UTF8String.EMPTY_UTF8)
            }
          val commitTimestamp = UTF8String.EMPTY_UTF8
          val commitSeqNo = UTF8String.EMPTY_UTF8
          val filename = UTF8String.EMPTY_UTF8

          // TODO use mutable row, avoid re-allocating
          new HoodieInternalRow(commitTimestamp, commitSeqNo, recordKey, partitionPath, filename, row, false)
        }
      }

    val metaFields = Seq(
      StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType),
      StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, StringType),
      StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType),
      StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, StringType),
      StructField(HoodieRecord.FILENAME_METADATA_FIELD, StringType))

    val updatedSchema = StructType(metaFields ++ schema.fields)

    val updatedDF = if (populateMetaFields && config.shouldCombineBeforeInsert) {
      val dedupedRdd = dedupeRows(prependedRdd, updatedSchema, config.getPreCombineField, SparkHoodieIndexFactory.isGlobalIndex(config))
      HoodieUnsafeUtils.createDataFrameFromRDD(df.sparkSession, dedupedRdd, updatedSchema)
    } else {
      HoodieUnsafeUtils.createDataFrameFromRDD(df.sparkSession, prependedRdd, updatedSchema)
    }

    val trimmedDF = if (shouldDropPartitionColumns) {
      dropPartitionColumns(updatedDF, config)
    } else {
      updatedDF
    }

    partitioner.repartitionRecords(trimmedDF, config.getBulkInsertShuffleParallelism)
  }

  private def dedupeRows(rdd: RDD[InternalRow], schema: StructType, preCombineFieldRef: String, isGlobalIndex: Boolean): RDD[InternalRow] = {
    val recordKeyMetaFieldOrd = schema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)
    val partitionPathMetaFieldOrd = schema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
    // NOTE: Pre-combine field could be a nested field
    val preCombineFieldPath = composeNestedFieldPath(schema, preCombineFieldRef)

    rdd.map { row =>
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
          val onePreCombineVal = getNestedInternalRowValue(oneRow, preCombineFieldPath).asInstanceOf[Comparable[AnyRef]]
          val otherPreCombineVal = getNestedInternalRowValue(otherRow, preCombineFieldPath).asInstanceOf[Comparable[AnyRef]]
          if (onePreCombineVal.compareTo(otherPreCombineVal.asInstanceOf[AnyRef]) >= 0) {
            oneRow
          } else {
            otherRow
          }
      }
      .values
  }

  private def dropPartitionColumns(df: DataFrame, config: HoodieWriteConfig): DataFrame = {
    val partitionPathFields = getPartitionPathFields(config).toSet
    val nestedPartitionPathFields = partitionPathFields.filter(f => f.contains('.'))
    if (nestedPartitionPathFields.nonEmpty) {
      logWarning(s"Can not drop nested partition path fields: $nestedPartitionPathFields")
    }

    val partitionPathCols = (partitionPathFields -- nestedPartitionPathFields).toSeq

    df.drop(partitionPathCols: _*)
  }

  private def getPartitionPathFields(config: HoodieWriteConfig): Seq[String] = {
    val keyGeneratorClassName = config.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME)
    val keyGenerator = ReflectionUtils.loadClass(keyGeneratorClassName, new TypedProperties(config.getProps)).asInstanceOf[BuiltinKeyGenerator]
    keyGenerator.getPartitionPathFields.asScala
  }
}
