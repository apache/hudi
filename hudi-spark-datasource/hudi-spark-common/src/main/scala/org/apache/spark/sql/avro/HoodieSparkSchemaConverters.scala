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

package org.apache.spark.sql.avro

import org.apache.hudi.HoodieSchemaConverters
import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.types.DataType

/**
 * This interface is simply a facade abstracting away Spark's [[HoodieSchemaConverters]] implementation, allowing
 * the rest of the code-base to not depend on it directly
 */
object HoodieSparkSchemaConverters extends HoodieSchemaConverters {

  override def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean) =
    HoodieSchemaInternalConverters.toSqlType(hoodieSchema)

  override def toHoodieType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): HoodieSchema =
    HoodieSchemaInternalConverters.toHoodieType(catalystType, nullable, recordName, nameSpace)
}
