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
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * NOTE: This code is borrowed from Spark 3.2.1
 *       This code is borrowed, so that we can better control compatibility w/in Spark minor
 *       branches (3.2.x, 3.1.x, etc)
 *
 *       PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 */
private[avro] object AvroUtils {

  /**
   * Wraps an Avro Schema object so that field lookups are faster.
   *
   * @param avroSchema           The schema in which to search for fields. Must be of type RECORD.
   * @param avroPath             The seq of parent field names leading to `avroSchema`.
   * @param positionalFieldMatch If true, perform field matching in a positional fashion
   *                             (structural comparison between schemas, ignoring names);
   *                             otherwise, perform field matching using field names.
   */
  class AvroSchemaHelper(avroSchema: Schema,
                         avroPath: Seq[String],
                         positionalFieldMatch: Boolean) {
    if (avroSchema.getType != Schema.Type.RECORD) {
      throw new IncompatibleSchemaException(
        s"Attempting to treat ${avroSchema.getName} as a RECORD, but it was: ${avroSchema.getType}")
    }

    private[this] val avroFieldArray = avroSchema.getFields.asScala.toArray
    private[this] val fieldMap = avroSchema.getFields.asScala
      .groupBy(_.name.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq) // toSeq needed for scala 2.13

    /**
     * Extract a single field from the contained avro schema which has the desired field name,
     * performing the matching with proper case sensitivity according to SQLConf.resolver.
     *
     * @param name The name of the field to search for.
     * @return `Some(match)` if a matching Avro field is found, otherwise `None`.
     */
    private[avro] def getFieldByName(name: String): Option[Schema.Field] = {

      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.name(), name)) match {
        case Seq(avroField) => Some(avroField)
        case Seq() => None
        case matches => throw new IncompatibleSchemaException(s"Searching for '$name' in Avro " +
          s"schema at ${toFieldStr(avroPath)} gave ${matches.size} matches. Candidates: " +
          matches.map(_.name()).mkString("[", ", ", "]")
        )
      }
    }

    /** Get the Avro field corresponding to the provided Catalyst field name/position, if any. */
    def getAvroField(fieldName: String, catalystPos: Int): Option[Schema.Field] = {
      if (positionalFieldMatch) {
        avroFieldArray.lift(catalystPos)
      } else {
        getFieldByName(fieldName)
      }
    }
  }


  /**
   * Take a field's hierarchical names (see [[toFieldStr]]) and position, and convert it to a
   * human-readable description of the field. Depending on the value of `positionalFieldMatch`,
   * either the position or name will be emphasized (for true and false, respectively); both will
   * be included in either case.
   */
  private[avro] def toFieldDescription(
                                        names: Seq[String],
                                        position: Int,
                                        positionalFieldMatch: Boolean): String = if (positionalFieldMatch) {
    s"field at position $position (${toFieldStr(names)})"
  } else {
    s"${toFieldStr(names)} (at position $position)"
  }

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private[avro] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

}
