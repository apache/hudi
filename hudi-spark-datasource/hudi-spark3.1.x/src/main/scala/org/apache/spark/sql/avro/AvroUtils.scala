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

import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

/**
 * NOTE: This code is borrowed from Spark 3.1.2
 * NOTE: This code is borrowed, so that we can better control compatibility w/in Spark minor
 *       branches (3.2.x, 3.1.x, etc)
 */
private[avro] object AvroUtils extends Logging {

  /**
   * Extract a single field from `avroSchema` which has the desired field name,
   * performing the matching with proper case sensitivity according to [[SQLConf.resolver]].
   *
   * @param avroSchema The schema in which to search for the field. Must be of type RECORD.
   * @param name The name of the field to search for.
   * @return `Some(match)` if a matching Avro field is found, otherwise `None`.
   * @throws IncompatibleSchemaException if `avroSchema` is not a RECORD or contains multiple
   *                                     fields matching `name` (i.e., case-insensitive matching
   *                                     is used and `avroSchema` has two or more fields that have
   *                                     the same name with difference case).
   */
  private[avro] def getAvroFieldByName(
                                        avroSchema: Schema,
                                        name: String): Option[Schema.Field] = {
    if (avroSchema.getType != Schema.Type.RECORD) {
      throw new IncompatibleSchemaException(
        s"Attempting to treat ${avroSchema.getName} as a RECORD, but it was: ${avroSchema.getType}")
    }
    avroSchema.getFields.asScala.filter(f => SQLConf.get.resolver(f.name(), name)).toSeq match {
      case Seq(avroField) => Some(avroField)
      case Seq() => None
      case matches => throw new IncompatibleSchemaException(
        s"Searching for '$name' in Avro schema gave ${matches.size} matches. Candidates: " +
          matches.map(_.name()).mkString("[", ", ", "]")
      )
    }
  }
}
