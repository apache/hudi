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

package org.apache.spark.sql.avro

import org.apache.avro.Schema.Type
import org.apache.avro.{AvroRuntimeException, Schema}

import scala.collection.JavaConverters._

/**
 * Serializes Catalyst payload into Avro object
 *
 * NOTE: This is low-level component operating on Spark internal data-types (comprising [[InternalRow]]).
 *       If you're looking to convert "deserialized" [[Row]] into Avro, please check [[AvroConversionUtils]]
 */
trait HoodieAvroSerializerTrait {
  def serialize(catalystData: Any): Any
}

object HoodieAvroSerializerTrait {
  /**
   * Check the nullability of the input Avro type and resolve it when it is nullable. The first
   * return value is a [[Boolean]] indicating if the input Avro type is nullable. The second
   * return value is either provided Avro type if it's not nullable, or its resolved non-nullable part
   * in case it is
   */
  def resolveAvroTypeNullability(avroType: Schema): (Boolean, Schema) = {
    if (avroType.getType == Type.UNION) {
      val fields = avroType.getTypes.asScala
      val actualType = fields.filter(_.getType != Type.NULL)
      if (fields.length != 2 || actualType.length != 1) {
        throw new AvroRuntimeException(
          s"Unsupported Avro UNION type $avroType: Only UNION of a null type and a non-null " +
            "type is supported")
      }
      (true, actualType.head)
    } else {
      (false, avroType)
    }
  }
}
