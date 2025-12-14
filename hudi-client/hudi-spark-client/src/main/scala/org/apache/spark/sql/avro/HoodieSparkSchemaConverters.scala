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

import org.apache.hudi.common.schema.HoodieSchema.TimePrecision
import org.apache.hudi.common.schema.{HoodieJsonProperties, HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * This object contains methods that are used to convert HoodieSchema to Spark SQL schemas and vice versa.
 *
 * This provides direct conversion between HoodieSchema and Spark DataType
 * without going through Avro Schema intermediary.
 *
 * NOTE: the package of this class is intentionally kept as "org.apache.spark.sql.avro" which is similar to the existing
 * Spark Avro connector's SchemaConverters.scala
 * (https://github.com/apache/spark/blob/master/connector/avro/src/main/scala/org/apache/spark/sql/avro/SchemaConverters.scala).
 * The reason for this is so that Spark 3.3 is able to access private spark sql type classes like TimestampNTZType.
 */

@DeveloperApi
object HoodieSparkSchemaConverters {

  /**
   * Internal wrapper for SQL data type and nullability.
   */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean) = {
    val result = toSqlTypeHelper(hoodieSchema, Set.empty)
    (result.dataType, result.nullable)
  }

  def toHoodieType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): HoodieSchema = {
    val schema = catalystType match {
      // Primitive types
      case BooleanType => HoodieSchema.create(HoodieSchemaType.BOOLEAN)
      case ByteType | ShortType | IntegerType => HoodieSchema.create(HoodieSchemaType.INT)
      case LongType => HoodieSchema.create(HoodieSchemaType.LONG)
      case DateType => HoodieSchema.createDate()
      case TimestampType => HoodieSchema.createTimestampMicros()
      case TimestampNTZType => HoodieSchema.createLocalTimestampMicros()
      case FloatType => HoodieSchema.create(HoodieSchemaType.FLOAT)
      case DoubleType => HoodieSchema.create(HoodieSchemaType.DOUBLE)
      case StringType | _: CharType | _: VarcharType => HoodieSchema.create(HoodieSchemaType.STRING)
      case NullType => HoodieSchema.create(HoodieSchemaType.NULL)
      case BinaryType => HoodieSchema.create(HoodieSchemaType.BYTES)

      case d: DecimalType =>
        val fixedSize = minBytesForPrecision(d.precision)
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        HoodieSchema.createDecimal(name, nameSpace, null, d.precision, d.scale, fixedSize)

      // Complex types
      case ArrayType(elementType, containsNull) =>
        val elementSchema = toHoodieType(elementType, containsNull, recordName, nameSpace)
        HoodieSchema.createArray(elementSchema)

      case MapType(StringType, valueType, valueContainsNull) =>
        val valueSchema = toHoodieType(valueType, valueContainsNull, recordName, nameSpace)
        HoodieSchema.createMap(valueSchema)

      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName

        // Check if this might be a union (using heuristic like Avro converter)
        if (canBeUnion(st)) {
          val nonNullUnionFieldTypes = st.map { f =>
            toHoodieType(f.dataType, nullable = false, f.name, childNameSpace)
          }
          val unionFieldTypes = if (nullable) {
            (HoodieSchema.create(HoodieSchemaType.NULL) +: nonNullUnionFieldTypes).asJava
          } else {
            nonNullUnionFieldTypes.asJava
          }
          HoodieSchema.createUnion(unionFieldTypes)
        } else {
          // Create record
          val fields = st.map { f =>
            val fieldSchema = toHoodieType(f.dataType, f.nullable, f.name, childNameSpace)
            val doc = f.getComment.orNull
            // Match existing Avro SchemaConverters behavior: use NULL_VALUE for nullable unions
            // to avoid serializing "default":null in JSON representation
            val defaultVal = if (fieldSchema.isNullable) {
              HoodieJsonProperties.NULL_VALUE
            } else {
              null
            }
            HoodieSchemaField.of(f.name, fieldSchema, doc, defaultVal)
          }

          HoodieSchema.createRecord(recordName, nameSpace, null, fields.asJava)
        }

      case other => throw new IncompatibleSchemaException(s"Unexpected Spark DataType: $other")
    }

    // Wrap with null union if nullable (and not already a union)
    if (nullable && catalystType != NullType && schema.getType != HoodieSchemaType.UNION) {
      HoodieSchema.createNullable(schema)
    } else {
      schema
    }
  }

  private def toSqlTypeHelper(hoodieSchema: HoodieSchema, existingRecordNames: Set[String]): SchemaType = {
    hoodieSchema.getType match {
      // Primitive types
      case HoodieSchemaType.INT => SchemaType(IntegerType, nullable = false)
      case HoodieSchemaType.STRING | HoodieSchemaType.ENUM => SchemaType(StringType, nullable = false)
      case HoodieSchemaType.BOOLEAN => SchemaType(BooleanType, nullable = false)
      case HoodieSchemaType.BYTES | HoodieSchemaType.FIXED => SchemaType(BinaryType, nullable = false)
      case HoodieSchemaType.DOUBLE => SchemaType(DoubleType, nullable = false)
      case HoodieSchemaType.FLOAT => SchemaType(FloatType, nullable = false)
      case HoodieSchemaType.LONG => SchemaType(LongType, nullable = false)
      case HoodieSchemaType.NULL => SchemaType(NullType, nullable = true)

      // Logical types
      case HoodieSchemaType.DATE =>
        SchemaType(DateType, nullable = false)

      case HoodieSchemaType.TIMESTAMP =>
        hoodieSchema match {
          case ts: HoodieSchema.Timestamp if !ts.isUtcAdjusted =>
            SchemaType(TimestampNTZType, nullable = false)
          case _ =>
            SchemaType(TimestampType, nullable = false)
        }

      case HoodieSchemaType.DECIMAL =>
        hoodieSchema match {
          case dec: HoodieSchema.Decimal =>
            SchemaType(DecimalType(dec.getPrecision, dec.getScale), nullable = false)
          case _ =>
            throw new IncompatibleSchemaException(
              s"DECIMAL type must be HoodieSchema.Decimal instance, got: ${hoodieSchema.getClass}")
        }

      case HoodieSchemaType.TIME =>
        hoodieSchema match {
          case time: HoodieSchema.Time =>
            time.getPrecision match {
              case TimePrecision.MILLIS => SchemaType(IntegerType, nullable = false)
              case TimePrecision.MICROS => SchemaType(LongType, nullable = false)
            }
          case _ =>
            throw new IncompatibleSchemaException(
              s"TIME type must be HoodieSchema.Time instance, got: ${hoodieSchema.getClass}")
        }

      case HoodieSchemaType.UUID =>
        SchemaType(StringType, nullable = false)

      // Complex types
      case HoodieSchemaType.RECORD =>
        val fullName = hoodieSchema.getFullName
        if (existingRecordNames.contains(fullName)) {
          throw new IncompatibleSchemaException(
            s"""
               |Found recursive reference in HoodieSchema, which cannot be processed by Spark:
               |$fullName
             """.stripMargin)
        }
        val newRecordNames = existingRecordNames + fullName
        val fields = hoodieSchema.getFields.asScala.map { f =>
          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          val metadata = if (f.doc().isPresent) {
            new MetadataBuilder().putString("comment", f.doc().get()).build()
          } else {
            Metadata.empty
          }
          StructField(f.name(), schemaType.dataType, schemaType.nullable, metadata)
        }
        SchemaType(StructType(fields.toSeq), nullable = false)

      case HoodieSchemaType.ARRAY =>
        val elementSchema = hoodieSchema.getElementType
        val schemaType = toSqlTypeHelper(elementSchema, existingRecordNames)
        SchemaType(ArrayType(schemaType.dataType, containsNull = schemaType.nullable), nullable = false)

      case HoodieSchemaType.MAP =>
        val valueSchema = hoodieSchema.getValueType
        val schemaType = toSqlTypeHelper(valueSchema, existingRecordNames)
        SchemaType(MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable), nullable = false)

      case HoodieSchemaType.UNION =>
        if (hoodieSchema.isNullable) {
          // Union with null - extract non-null type and mark as nullable
          val types = hoodieSchema.getTypes.asScala
          val remainingTypes = types.filter(_.getType != HoodieSchemaType.NULL)
          if (remainingTypes.size == 1) {
            toSqlTypeHelper(remainingTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            toSqlTypeHelper(HoodieSchema.createUnion(remainingTypes.asJava), existingRecordNames)
              .copy(nullable = true)
          }
        } else {
          // Union without null - handle type promotions and member structs
          val types = hoodieSchema.getTypes.asScala
          types.map(_.getType).toSeq match {
            case Seq(t) =>
              toSqlTypeHelper(types.head, existingRecordNames)
            case Seq(t1, t2) if Set(t1, t2) == Set(HoodieSchemaType.INT, HoodieSchemaType.LONG) =>
              SchemaType(LongType, nullable = false)
            case Seq(t1, t2) if Set(t1, t2) == Set(HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE) =>
              SchemaType(DoubleType, nullable = false)
            case _ =>
              // Convert to struct with member0, member1, ... fields (like Avro union handling)
              val fields = types.zipWithIndex.map {
                case (s, i) =>
                  val schemaType = toSqlTypeHelper(s, existingRecordNames)
                  StructField(s"member$i", schemaType.dataType, nullable = true)
              }
              SchemaType(StructType(fields.toSeq), nullable = false)
          }
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported HoodieSchemaType: $other")
    }
  }

  private def canBeUnion(st: StructType): Boolean = {
    st.fields.length > 0 &&
      st.forall { f =>
        f.name.matches("member\\d+") && f.nullable
      }
  }
}
