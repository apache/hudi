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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType


/**
 * Intended to be used just with HoodieSparkParquetReader to avoid any java/scala issues
 */
class SparkBasicSchemaEvolution(fileSchema: StructType,
                                requiredSchema: StructType,
                                sessionLocalTimeZone: String) {

  val (implicitTypeChangeInfo, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

  def getRequestSchema: StructType = {
    if (implicitTypeChangeInfo.isEmpty) {
      requiredSchema
    } else {
      sparkRequestSchema
    }
  }

  def generateUnsafeProjection(): UnsafeProjection = {
    val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils
    HoodieParquetFileFormatHelper.generateUnsafeProjection(schemaUtils.toAttributes(requiredSchema), Some(sessionLocalTimeZone),
      implicitTypeChangeInfo, requiredSchema, new StructType(), schemaUtils)
  }
}
