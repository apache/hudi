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

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * Represents a pair of matched fields between Catalyst (Spark) schema and HoodieSchema.
 */
private[sql] case class HoodieMatchedField(
  catalystField: org.apache.spark.sql.types.StructField,
  catalystPosition: Int,
  hoodieField: HoodieSchemaField)

/**
 * Helper class to perform field lookup/matching between HoodieSchema and Catalyst schemas.
 *
 * This will match `hoodieSchema` against `catalystSchema`, attempting to find a matching field in
 * the Hoodie schema for each field in the Catalyst schema and vice-versa, respecting settings for
 * case sensitivity. The match results can be accessed using the getter methods.
 *
 * @param hoodieSchema The schema in which to search for fields. Must be of type RECORD.
 * @param catalystSchema The Catalyst schema to use for matching.
 * @param hoodieSchemaPath The seq of parent field names leading to `hoodieSchema`.
 * @param catalystPath The seq of parent field names leading to `catalystSchema`.
 * @param positionalFieldMatch If true, perform field matching in a positional fashion
 *                             (structural comparison between schemas, ignoring names);
 *                             otherwise, perform field matching using field names.
 */
private[sql] class HoodieSchemaHelper(
  hoodieSchema: HoodieSchema,
  catalystSchema: StructType,
  hoodieSchemaPath: Seq[String],
  catalystPath: Seq[String],
  positionalFieldMatch: Boolean) {

  if (hoodieSchema.getType != HoodieSchemaType.RECORD) {
    throw new IncompatibleSchemaException(
      s"Attempting to treat ${hoodieSchema.getName} as a RECORD, but it was: ${hoodieSchema.getType}")
  }

  private[this] val hoodieFieldArray = hoodieSchema.getFields.asScala.toArray
  private[this] val fieldMap = hoodieSchema.getFields.asScala
    .groupBy(_.name().toLowerCase(Locale.ROOT))
    .mapValues(_.toSeq) // toSeq needed for scala 2.13

  /** The fields which have matching equivalents in both Hoodie and Catalyst schemas. */
  val matchedFields: Seq[HoodieMatchedField] = catalystSchema.zipWithIndex.flatMap {
    case (sqlField, sqlPos) =>
      getHoodieField(sqlField.name, sqlPos).map(HoodieMatchedField(sqlField, sqlPos, _))
  }

  /**
   * Validate that there are no Catalyst fields which don't have a matching Hoodie field, throwing
   * [[IncompatibleSchemaException]] if such extra fields are found. If `ignoreNullable` is false,
   * consider nullable Catalyst fields to be eligible to be an extra field; otherwise,
   * ignore nullable Catalyst fields when checking for extras.
   */
  def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
    catalystSchema.zipWithIndex.foreach { case (sqlField, sqlPos) =>
      if (getHoodieField(sqlField.name, sqlPos).isEmpty &&
        (!ignoreNullable || !sqlField.nullable)) {
        if (positionalFieldMatch) {
          throw new IncompatibleSchemaException("Cannot find field at position " +
            s"$sqlPos of ${toFieldStr(hoodieSchemaPath)} from Hoodie schema (using positional matching)")
        } else {
          throw new IncompatibleSchemaException(
            s"Cannot find ${toFieldStr(catalystPath :+ sqlField.name)} in Hoodie schema")
        }
      }
    }

  /**
   * Validate that there are no Hoodie fields which don't have a matching Catalyst field, throwing
   * [[IncompatibleSchemaException]] if such extra fields are found. Only required (non-nullable)
   * fields are checked; nullable fields are ignored.
   */
  def validateNoExtraRequiredHoodieFields(): Unit = {
    val extraFields = hoodieFieldArray.toSet -- matchedFields.map(_.hoodieField)
    extraFields.filterNot(isNullable).foreach { extraField =>
      if (positionalFieldMatch) {
        throw new IncompatibleSchemaException(s"Found field '${extraField.name()}' at position " +
          s"${extraField.pos()} of ${toFieldStr(hoodieSchemaPath)} from Hoodie schema but there is no " +
          s"match in the SQL schema at ${toFieldStr(catalystPath)} (using positional matching)")
      } else {
        throw new IncompatibleSchemaException(
          s"Found ${toFieldStr(hoodieSchemaPath :+ extraField.name())} in Hoodie schema but there is no " +
            "match in the SQL schema")
      }
    }
  }

  /**
   * Extract a single field from the contained hoodie schema which has the desired field name,
   * performing the matching with proper case sensitivity according to SQLConf.resolver.
   *
   * @param name The name of the field to search for.
   * @return `Some(match)` if a matching Hoodie field is found, otherwise `None`.
   */
  private[avro] def getFieldByName(name: String): Option[HoodieSchemaField] = {
    // get candidates, ignoring case of field name
    val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

    // search candidates, taking into account case sensitivity settings
    candidates.filter(f => SQLConf.get.resolver(f.name(), name)) match {
      case Seq(hoodieField) => Some(hoodieField)
      case Seq() => None
      case matches => throw new IncompatibleSchemaException(s"Searching for '$name' in Hoodie " +
        s"schema at ${toFieldStr(hoodieSchemaPath)} gave ${matches.size} matches. Candidates: " +
        matches.map(_.name()).mkString("[", ", ", "]")
      )
    }
  }

  /** Get the Hoodie field corresponding to the provided Catalyst field name/position, if any. */
  def getHoodieField(fieldName: String, catalystPos: Int): Option[HoodieSchemaField] = {
    if (positionalFieldMatch) {
      hoodieFieldArray.lift(catalystPos)
    } else {
      getFieldByName(fieldName)
    }
  }

  /** Return true iff `hoodieField` is nullable, i.e. `UNION` type and has `NULL` as an option. */
  private def isNullable(hoodieField: HoodieSchemaField): Boolean =
    hoodieField.schema().getType == HoodieSchemaType.UNION &&
      hoodieField.schema().getTypes.asScala.exists(_.getType == HoodieSchemaType.NULL)

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }
}
