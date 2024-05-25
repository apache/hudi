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
 *
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.avro.HoodieAvroUtils.rewriteRecordWithNewSchema

abstract class AvroProjection extends (GenericRecord => GenericRecord)

object AvroProjection {

  /**
   * Creates projection into provided [[Schema]] allowing to convert [[GenericRecord]] into
   * new schema
   */
  def create(schema: Schema): AvroProjection = {
    val projection = (record: GenericRecord) => rewriteRecordWithNewSchema(record, schema)
    // NOTE: Have to use explicit [[Projection]] instantiation to stay compatible w/ Scala 2.11
    new AvroProjection {
      override def apply(record: GenericRecord): GenericRecord =
        if (record.getSchema == schema) {
          record
        } else {
          projection(record)
        }
    }
  }

}
