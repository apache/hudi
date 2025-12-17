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
import org.apache.avro.file.FileReader
import org.apache.avro.generic.GenericRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * NOTE: This code is borrowed from Spark 3.3.0
 *       This code is borrowed, so that we can better control compatibility w/in Spark minor
 *       branches (3.2.x, 3.1.x, etc)
 *
 *       PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 */
private[sql] object AvroUtils extends Logging {

  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  // The trait provides iterator-like interface for reading records from an Avro file,
  // deserializing and returning them as internal rows.
  trait RowReader {
    protected val fileReader: FileReader[GenericRecord]
    protected val deserializer: AvroDeserializerInternal
    protected val stopPosition: Long

    private[this] var completed = false
    private[this] var currentRow: Option[InternalRow] = None

    def hasNextRow: Boolean = {
      while (!completed && currentRow.isEmpty) {
        val r = fileReader.hasNext && !fileReader.pastSync(stopPosition)
        if (!r) {
          fileReader.close()
          completed = true
          currentRow = None
        } else {
          val record = fileReader.next()
          // the row must be deserialized in hasNextRow, because AvroDeserializer#deserialize
          // potentially filters rows
          currentRow = deserializer.deserialize(record).asInstanceOf[Option[InternalRow]]
        }
      }
      currentRow.isDefined
    }

    def nextRow: InternalRow = {
      if (currentRow.isEmpty) {
        hasNextRow
      }
      val returnRow = currentRow
      currentRow = None // free up hasNextRow to consume more Avro records, if not exhausted
      returnRow.getOrElse {
        throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Avro field. */
  private[sql] case class AvroMatchedField(
                                            catalystField: StructField,
                                            catalystPosition: Int,
                                            avroField: Schema.Field)

  /**
   * Helper class to perform field lookup/matching on Avro schemas.
   *
   * This will match `avroSchema` against `catalystSchema`, attempting to find a matching field in
   * the Avro schema for each field in the Catalyst schema and vice-versa, respecting settings for
   * case sensitivity. The match results can be accessed using the getter methods.
   *
   * @param avroSchema The schema in which to search for fields. Must be of type RECORD.
   * @param catalystSchema The Catalyst schema to use for matching.
   * @param avroPath The seq of parent field names leading to `avroSchema`.
   * @param catalystPath The seq of parent field names leading to `catalystSchema`.
   * @param positionalFieldMatch If true, perform field matching in a positional fashion
   *                             (structural comparison between schemas, ignoring names);
   *                             otherwise, perform field matching using field names.
   */
  class AvroSchemaHelper(
                          avroSchema: Schema,
                          catalystSchema: StructType,
                          avroPath: Seq[String],
                          catalystPath: Seq[String],
                          positionalFieldMatch: Boolean) {
    if (avroSchema.getType != Schema.Type.RECORD) {
      throw new IncompatibleSchemaException(
        s"Attempting to treat ${avroSchema.getName} as a RECORD, but it was: ${avroSchema.getType}")
    }

    private[this] val avroFieldArray = avroSchema.getFields.asScala.toArray
    private[this] val fieldMap = avroSchema.getFields.asScala
      .groupBy(_.name.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq) // toSeq needed for scala 2.13

    /** The fields which have matching equivalents in both Avro and Catalyst schemas. */
    val matchedFields: Seq[AvroMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) =>
        getAvroField(sqlField.name, sqlPos).map(AvroMatchedField(sqlField, sqlPos, _))
    }

    /**
     * Validate that there are no Catalyst fields which don't have a matching Avro field, throwing
     * [[IncompatibleSchemaException]] if such extra fields are found. If `ignoreNullable` is false,
     * consider nullable Catalyst fields to be eligible to be an extra field; otherwise,
     * ignore nullable Catalyst fields when checking for extras.
     */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.zipWithIndex.foreach { case (sqlField, sqlPos) =>
        if (getAvroField(sqlField.name, sqlPos).isEmpty &&
          (!ignoreNullable || !sqlField.nullable)) {
          if (positionalFieldMatch) {
            throw new IncompatibleSchemaException("Cannot find field at position " +
              s"$sqlPos of ${toFieldStr(avroPath)} from Avro schema (using positional matching)")
          } else {
            throw new IncompatibleSchemaException(
              s"Cannot find ${toFieldStr(catalystPath :+ sqlField.name)} in Avro schema")
          }
        }
      }

    /**
     * Validate that there are no Avro fields which don't have a matching Catalyst field, throwing
     * [[IncompatibleSchemaException]] if such extra fields are found. Only required (non-nullable)
     * fields are checked; nullable fields are ignored.
     */
    def validateNoExtraRequiredAvroFields(): Unit = {
      val extraFields = avroFieldArray.toSet -- matchedFields.map(_.avroField)
      extraFields.filterNot(isNullable).foreach { extraField =>
        if (positionalFieldMatch) {
          throw new IncompatibleSchemaException(s"Found field '${extraField.name()}' at position " +
            s"${extraField.pos()} of ${toFieldStr(avroPath)} from Avro schema but there is no " +
            s"match in the SQL schema at ${toFieldStr(catalystPath)} (using positional matching)")
        } else {
          throw new IncompatibleSchemaException(
            s"Found ${toFieldStr(avroPath :+ extraField.name())} in Avro schema but there is no " +
              "match in the SQL schema")
        }
      }
    }

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
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private[avro] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  /** Return true iff `avroField` is nullable, i.e. `UNION` type and has `NULL` as an option. */
  private[avro] def isNullable(avroField: Schema.Field): Boolean =
    avroField.schema().getType == Schema.Type.UNION &&
      avroField.schema().getTypes.asScala.exists(_.getType == Schema.Type.NULL)
}
