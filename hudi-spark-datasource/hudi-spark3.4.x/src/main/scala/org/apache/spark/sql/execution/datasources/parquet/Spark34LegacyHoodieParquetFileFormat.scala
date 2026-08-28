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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Spark 3.4 concrete implementation of [[Spark3LegacyHoodieParquetFileFormat]]. It only overrides
 * the version-specific hooks; the shared reader logic lives in the base class.
 */
class Spark34LegacyHoodieParquetFileFormat(appendPartitionValues: Boolean)
  extends Spark3LegacyHoodieParquetFileFormat(appendPartitionValues) {

  def supportsColumnar(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    // Only output columnar if there is WSCG to read it.
    val requiredWholeStageCodegenSettings =
      conf.wholeStageEnabled && !WholeStageCodegenExec.isTooManyFields(conf, schema)
    requiredWholeStageCodegenSettings &&
      supportBatch(sparkSession, schema)
  }

  override protected def toAttributes(structType: StructType): Seq[Attribute] =
    structType.toAttributes

  override protected def getFilePath(file: PartitionedFile): Path =
    file.filePath.toPath

  override protected def isVectorizedReaderEnabled(sparkSession: SparkSession,
                                                   resultSchema: StructType): Boolean =
    supportBatch(sparkSession, resultSchema)

  override protected def getPushDownStringPredicate(sqlConf: SQLConf): Boolean =
    sqlConf.parquetFilterPushDownStringPredicate

  override protected def getReturningBatch(sparkSession: SparkSession,
                                           resultSchema: StructType): Boolean =
    sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
      supportsColumnar(sparkSession, resultSchema).toString.equals("true")

  override protected def setParquetTimeConfs(hadoopConf: Configuration, sparkSession: SparkSession): Unit = {
    // Using string value of this conf to preserve compatibility across spark versions.
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.getConfString(
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.defaultValueString).toBoolean
    )
    hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, sparkSession.sessionState.conf.legacyParquetNanosAsLong)
  }
}
