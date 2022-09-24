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

import org.apache.hudi.SafeAvroProjection.collectFieldOrdinals
import org.apache.hudi.common.util.ValidationUtils.checkState

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.collection.JavaConverters._

// TODO extract to HoodieAvroSchemaUtils
abstract class AvroProjection extends (GenericRecord => GenericRecord)

class SafeAvroProjection(sourceSchema: Schema,
                         projectedSchema: Schema,
                         reusableRecordBuilder: GenericRecordBuilder = null) extends AvroProjection {

  private val ordinals: List[Int] = collectFieldOrdinals(projectedSchema, sourceSchema)
  private val recordBuilder: GenericRecordBuilder =
    if (reusableRecordBuilder != null) {
      reusableRecordBuilder
    } else {
      new GenericRecordBuilder(projectedSchema)
    }

  override def apply(record: GenericRecord): GenericRecord = {
    val fields = projectedSchema.getFields.asScala
    checkState(fields.length == ordinals.length)
    fields.zip(ordinals).foreach {
      case (field, pos) => recordBuilder.set(field, record.get(pos))
    }
    recordBuilder.build()
  }
}

object SafeAvroProjection {
  def create(sourceSchema: Schema, projectedSchema: Schema, reusableRecordBuilder: GenericRecordBuilder = null): SafeAvroProjection =
    new SafeAvroProjection(
      sourceSchema = sourceSchema,
      projectedSchema = projectedSchema,
      reusableRecordBuilder = reusableRecordBuilder)

  /**
   * Maps [[projected]] [[Schema]] onto [[source]] one, collecting corresponding field ordinals w/in it, which
   * will be subsequently used by either [[projectRowUnsafe]] or [[projectAvroUnsafe()]] method
   *
   * @param projected target projected schema (which is a proper subset of [[source]] [[Schema]])
   * @param source source schema of the record being projected
   * @return list of ordinals of corresponding fields of [[projected]] schema w/in [[source]] one
   */
  private def collectFieldOrdinals(projected: Schema, source: Schema): List[Int] = {
    projected.getFields.asScala.map(f => source.getField(f.name()).pos()).toList
  }
}
