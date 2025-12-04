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

package org.apache.hudi

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * Base Spark 3.x implementation for converting HoodieSchema to Spark SQL schemas.
 *
 * This implementation works for Spark 3.3+ but does NOT use TimestampNTZType.
 * Local timestamps are mapped to regular TimestampType for compatibility.
 * Spark 3.4+ can override to use TimestampNTZType.
 */
object BaseSpark3HoodieSchemaConverters extends HoodieSchemaConverters {

  /**
   * Internal wrapper for SQL data type and nullability.
   */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  override def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean) = {
    val result = toSqlTypeHelper(hoodieSchema, Set.empty)
    (result.dataType, result.nullable)
  }

  override def toHoodieType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): HoodieSchema = {
    val schema = catalystType match {
      // Primitive types
      case BooleanType => HoodieSchema.create(HoodieSchemaType.BOOLEAN)
      case ByteType | ShortType | IntegerType => HoodieSchema.create(HoodieSchemaType.INT)
      case LongType => HoodieSchema.create(HoodieSchemaType.LONG)
      case DateType => HoodieSchema.createDate()
      case TimestampType => HoodieSchema.createTimestampMicros()
      case FloatType => HoodieSchema.create(HoodieSchemaType.FLOAT)
      case DoubleType => HoodieSchema.create(HoodieSchemaType.DOUBLE)
      case StringType | _: CharType | _: VarcharType => HoodieSchema.create(HoodieSchemaType.STRING)
      case NullType => HoodieSchema.create(HoodieSchemaType.NULL)
      case BinaryType => HoodieSchema.create(HoodieSchemaType.BYTES)

      case d: DecimalType =>
        HoodieSchema.createDecimal(d.precision, d.scale)

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
            val types = new java.util.ArrayList[HoodieSchema]()
            types.add(HoodieSchema.create(HoodieSchemaType.NULL))
            nonNullUnionFieldTypes.foreach(types.add)
            types
          } else {
            val types = new java.util.ArrayList[HoodieSchema]()
            nonNullUnionFieldTypes.foreach(types.add)
            types
          }
          HoodieSchema.createUnion(unionFieldTypes)
        } else {
          // Create record
          val fields = st.map { f =>
            val fieldSchema = toHoodieType(f.dataType, f.nullable, f.name, childNameSpace)
            val doc = if (f.metadata.contains("comment")) f.metadata.getString("comment") else null
            HoodieSchemaField.of(f.name, fieldSchema, doc)
          }

          val fieldsJava = new java.util.ArrayList[HoodieSchemaField]()
          fields.foreach(fieldsJava.add)

          HoodieSchema.createRecord(recordName, nameSpace, null, fieldsJava)
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
      case HoodieSchemaType.STRING => SchemaType(StringType, nullable = false)
      case HoodieSchemaType.BOOLEAN => SchemaType(BooleanType, nullable = false)
      case HoodieSchemaType.BYTES => SchemaType(BinaryType, nullable = false)
      case HoodieSchemaType.DOUBLE => SchemaType(DoubleType, nullable = false)
      case HoodieSchemaType.FLOAT => SchemaType(FloatType, nullable = false)
      case HoodieSchemaType.LONG => SchemaType(LongType, nullable = false)
      case HoodieSchemaType.NULL => SchemaType(NullType, nullable = true)
      case HoodieSchemaType.ENUM => SchemaType(StringType, nullable = false)

      // Logical types
      case HoodieSchemaType.DATE =>
        SchemaType(DateType, nullable = false)

      case HoodieSchemaType.TIMESTAMP =>
        hoodieSchema match {
          case ts: HoodieSchema.Timestamp =>
            // For Spark 3.3: Map both UTC and local timestamps to TimestampType
            // (Spark 3.4+ will override this to use TimestampNTZType for local)
            SchemaType(TimestampType, nullable = false)
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

      case HoodieSchemaType.FIXED =>
        // FIXED can be either binary or decimal with logical type
        hoodieSchema match {
          case dec: HoodieSchema.Decimal =>
            SchemaType(DecimalType(dec.getPrecision, dec.getScale), nullable = false)
          case _ =>
            SchemaType(BinaryType, nullable = false)
        }

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
          StructField(f.name(), schemaType.dataType, schemaType.nullable)
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

/**
 * Exception thrown when schemas are incompatible during conversion.
 */
class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
