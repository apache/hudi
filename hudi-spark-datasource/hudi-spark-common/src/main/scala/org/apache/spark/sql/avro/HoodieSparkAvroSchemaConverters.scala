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

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters.SchemaType
import org.apache.spark.sql.types.DataType

/**
 * This interface is simply a facade abstracting away Spark's [[SchemaConverters]] implementation, allowing
 * the rest of the code-base to not depend on it directly
 */
object HoodieSparkAvroSchemaConverters extends HoodieAvroSchemaConverters {

  override def toSqlType(avroSchema: Schema): (DataType, Boolean, Option[String]) =
    SchemaConverters.toSqlType(avroSchema) match {
      case SchemaType(dataType, nullable, doc) => (dataType, nullable, doc)
    }

  override def toAvroType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): Schema =
    SchemaConverters.toAvroType(catalystType, nullable, recordName, nameSpace)

}
