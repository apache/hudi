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

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}

import org.apache.spark.sql.types._

/**
 * Spark 3.5+ implementation that supports TimestampNTZType for local timestamps.
 *
 * Extends the base Spark 3 implementation and adds TimestampNTZType support.
 */
object Spark35HoodieSchemaConverters extends HoodieSchemaConverters {

  override def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean) = {
    // Use base implementation but post-process for TimestampNTZ
    val (dataType, nullable) = BaseSpark3HoodieSchemaConverters.toSqlType(hoodieSchema)

    // If it's a local timestamp (non-UTC), convert to TimestampNTZType
    val adjustedDataType = if (dataType == TimestampType && hoodieSchema.getType == HoodieSchemaType.TIMESTAMP) {
      hoodieSchema match {
        case ts: HoodieSchema.Timestamp if !ts.isUtcAdjusted =>
          TimestampNTZType
        case _ =>
          dataType
      }
    } else {
      dataType
    }

    (adjustedDataType, nullable)
  }

  override def toHoodieType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): HoodieSchema = {
    catalystType match {
      // Handle TimestampNTZType (Spark 3.4+ only)
      case TimestampNTZType =>
        HoodieSchema.createLocalTimestampMicros()

      // Delegate everything else to base implementation
      case _ =>
        BaseSpark3HoodieSchemaConverters.toHoodieType(catalystType, nullable, recordName, nameSpace)
    }
  }
}
