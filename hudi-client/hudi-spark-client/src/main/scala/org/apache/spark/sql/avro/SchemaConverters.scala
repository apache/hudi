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

import org.apache.hudi.avro.{AvroSchemaUtils, HoodieAvroUtils}
import org.apache.hudi.common.util.StringUtils

import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.LogicalTypes.{Date, Decimal, LocalTimestampMicros, LocalTimestampMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal.minBytesForPrecision

import scala.collection.JavaConverters._

/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 *
 * NOTE: This code is borrowed from Spark 3.2.1
 *       This code is borrowed, so that we can better control compatibility w/in Spark minor
 *       branches (3.2.x, 3.1.x, etc)
 *
 *       PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 */
@DeveloperApi
private[sql] object SchemaConverters {
  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  /**
   * Internal wrapper for SQL data type and nullability.
   *
   * @since 2.4.0
   */
  case class SchemaType(dataType: DataType, nullable: Boolean, doc: Option[String])

  /**
   * Converts an Avro schema to a corresponding Spark SQL schema.
   *
   * @since 2.4.0
   */
  def toSqlType(avroSchema: Schema): SchemaType = {
    toSqlTypeHelper(avroSchema, Set.empty)
  }

  private val unionFieldMemberPrefix = "member"

  private def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {
    (avroSchema.getType, Option(avroSchema.getDoc)) match {
      case (INT, doc) => avroSchema.getLogicalType match {
        case _: Date => SchemaType(DateType, nullable = false, doc)
        case _ => SchemaType(IntegerType, nullable = false, doc)
      }
      case (STRING, doc) => SchemaType(StringType, nullable = false, doc)
      case (BOOLEAN, doc) => SchemaType(BooleanType, nullable = false, doc)
      case (BYTES | FIXED, doc) => avroSchema.getLogicalType match {
        // For FIXED type, if the precision requires more bytes than fixed size, the logical
        // type will be null, which is handled by Avro library.
        case d: Decimal => SchemaType(DecimalType(d.getPrecision, d.getScale), nullable = false, doc)
        case _ => SchemaType(BinaryType, nullable = false, doc)
      }

      case (DOUBLE, doc) => SchemaType(DoubleType, nullable = false, doc)
      case (FLOAT, doc) => SchemaType(FloatType, nullable = false, doc)
      case (LONG, doc) => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(TimestampType, nullable = false, doc)
        case _: LocalTimestampMillis | _: LocalTimestampMicros => SchemaType(TimestampNTZType, nullable = false, doc)
        case _ => SchemaType(LongType, nullable = false, doc)
      }

      case (ENUM, doc) => SchemaType(StringType, nullable = false, doc)

      case (NULL, doc) => SchemaType(NullType, nullable = true, doc)

      case (RECORD, doc) =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(
            s"""
               |Found recursive reference in Avro schema, which can not be processed by Spark:
               |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          val metadata = if (StringUtils.isNullOrEmpty(f.doc())) {
            Metadata.empty
          } else {
            new MetadataBuilder().putString("comment", f.doc()).build()
          }
          StructField(f.name, schemaType.dataType, schemaType.nullable, metadata)
        }

        SchemaType(StructType(fields.toSeq), nullable = false, doc)

      case (ARRAY, doc) =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false, doc)

      case (MAP, doc) =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false, doc)

      case (UNION, doc) =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlTypeHelper(remainingUnionTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            toSqlTypeHelper(Schema.createUnion(remainingUnionTypes.asJava), existingRecordNames)
              .copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType).toSeq match {
          case Seq(t1) =>
            toSqlTypeHelper(avroSchema.getTypes.get(0), existingRecordNames)
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false, doc)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false, doc)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                // All fields are nullable because only one of them is set at a time
                val metadata = if(schemaType.doc.isDefined) new MetadataBuilder().putString("comment", schemaType.doc.get).build() else Metadata.empty
                StructField(s"$unionFieldMemberPrefix$i", schemaType.dataType, nullable = true, metadata)
            }

            SchemaType(StructType(fields.toSeq), nullable = false, doc)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  /**
   * Converts a Spark SQL schema to a corresponding Avro schema.
   *
   * @since 2.4.0
   */
  def toAvroType(catalystType: DataType,
                 nullable: Boolean = false,
                 recordName: String = "topLevelRecord",
                 nameSpace: String = ""): Schema = {

    val schema = catalystType match {
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case ByteType | ShortType | IntegerType => Schema.create(Schema.Type.INT)
      case LongType => Schema.create(Schema.Type.LONG)
      case DateType =>
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
      case TimestampNTZType =>
        LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))

      case FloatType => Schema.create(Schema.Type.FLOAT)
      case DoubleType => Schema.create(Schema.Type.DOUBLE)
      case StringType | CharType(_) | VarcharType(_) => Schema.create(Schema.Type.STRING)
      case NullType => Schema.create(Schema.Type.NULL)
      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case BinaryType => Schema.create(Schema.Type.BYTES)
      case ArrayType(et, containsNull) =>
        Schema.createArray(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        Schema.createMap(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case struct: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        if (canBeUnion(struct)) {
          val nonNullUnionFieldTypes = struct.map(f => toAvroType(f.dataType, nullable = false, f.name, childNameSpace))
          val unionFieldTypes = if (nullable) {
            nullSchema +: nonNullUnionFieldTypes
          } else {
            nonNullUnionFieldTypes
          }
          Schema.createUnion(unionFieldTypes:_*)
        } else {
          val fields = struct.map { field =>
            val doc = field.getComment().orNull
            val fieldAvroType = toAvroType(field.dataType, field.nullable, field.name, childNameSpace)
            if (AvroSchemaUtils.isNullable(fieldAvroType)) {
              HoodieAvroUtils.createNewSchemaField(field.name, fieldAvroType, doc, JsonProperties.NULL_VALUE)
            } else {
              HoodieAvroUtils.createNewSchemaField(field.name, fieldAvroType, doc, null)
            }
          }
          Schema.createRecord(recordName, null, nameSpace, false, fields.asJava)
        }

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }

    if (nullable && catalystType != NullType && schema.getType != Schema.Type.UNION) {
      Schema.createUnion(nullSchema, schema)
    } else {
      schema
    }
  }

  private def canBeUnion(st: StructType): Boolean = {
    // We use a heuristic to determine whether a [[StructType]] could potentially have been produced
    // by converting Avro union to Catalyst's [[StructType]]:
    //    - It has to have at least 1 field
    //    - All fields have to be of the following format "memberN" (where N is sequentially increasing integer)
    //    - All fields are nullable
    st.fields.length > 0 &&
    st.forall { f =>
      f.name.matches(s"$unionFieldMemberPrefix\\d+") && f.nullable
    }
  }
}

private[avro] class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

private[avro] class UnsupportedAvroTypeException(msg: String) extends Exception(msg)
