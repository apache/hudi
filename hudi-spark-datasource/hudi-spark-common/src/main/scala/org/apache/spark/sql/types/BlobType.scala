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

package org.apache.spark.sql.types

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.avro.HoodieSparkSchemaConverters

/**
 * Factory object for creating Spark StructType representation of {@link HoodieSchema.Blob}.
 *
 * BLOB is a logical type in Hudi that represents binary large objects. The data can be
 * stored inline (as bytes) or out-of-line (as a reference to a file location).
 */
object BlobType {

  /**
   * Creates a StructType representing a {@link HoodieSchema.Blob}.
   *
   * @return StructType with blob structure
   */
  def apply(): DataType = {
    HoodieSparkSchemaConverters.toSqlType(HoodieSchema.createBlob())._1
  }
}
