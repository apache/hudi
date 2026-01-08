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

import org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.Try

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

  // Reflection-based checks for types that may not be available in all Avro/Spark versions
  private lazy val localTimestampMillisClass: Option[Class[_]] = Try {
    Class.forName("org.apache.avro.LogicalTypes$LocalTimestampMillis")
  }.toOption

  private lazy val localTimestampMicrosClass: Option[Class[_]] = Try {
    Class.forName("org.apache.avro.LogicalTypes$LocalTimestampMicros")
  }.toOption

  private lazy val timestampNTZTypeClass: Option[Class[_]] = Try {
    Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
  }.toOption

  private lazy val timestampNTZTypeInstance: Option[DataType] = timestampNTZTypeClass.flatMap { clazz =>
    Try {
      val module = clazz.getField("MODULE$").get(null)
      module.asInstanceOf[DataType]
    }.toOption
  }

  private lazy val localTimestampMicrosMethod: Option[java.lang.reflect.Method] = Try {
    classOf[LogicalTypes].getMethod("localTimestampMicros")
  }.toOption

  private lazy val localTimestampMillisMethod: Option[java.lang.reflect.Method] = Try {
    classOf[LogicalTypes].getMethod("localTimestampMillis")
  }.toOption

  /**
   * Checks if a logical type is an instance of LocalTimestampMillis using reflection.
   * Returns false if the class doesn't exist (e.g., in Avro 1.8.2).
   */
  private def isLocalTimestampMillis(logicalType: LogicalType): Boolean = {
    logicalType != null && localTimestampMillisClass.exists(_.isInstance(logicalType))
  }

  /**
   * Checks if a logical type is an instance of LocalTimestampMicros using reflection.
   * Returns false if the class doesn't exist (e.g., in Avro 1.8.2).
   */
  private def isLocalTimestampMicros(logicalType: LogicalType): Boolean = {
    logicalType != null && localTimestampMicrosClass.exists(_.isInstance(logicalType))
  }

  /**
   * Checks if a DataType is TimestampNTZType using reflection.
   * Returns false if the class doesn't exist (e.g., in Spark 2.x or Spark 3.2).
   */
  private def isTimestampNTZType(dataType: DataType): Boolean = {
    timestampNTZTypeInstance.contains(dataType) ||
      (timestampNTZTypeClass.isDefined && timestampNTZTypeClass.get.isInstance(dataType))
  }

  /**
   * Creates a LocalTimestampMicros schema using reflection.
   * Throws UnsupportedOperationException if not available.
   */
  private def createLocalTimestampMicrosSchema(): Schema = {
    localTimestampMicrosMethod match {
      case Some(method) =>
        val logicalType = method.invoke(null).asInstanceOf[LogicalType]
        logicalType.addToSchema(Schema.create(Schema.Type.LONG))
      case None =>
        throw new UnsupportedOperationException("LocalTimestampMicros is not supported in this Avro version")
    }
  }

  /**
   * Internal wrapper for SQL data type and nullability.
   *
   * @since 2.4.0
   */
  case class SchemaType(dataType: DataType, nullable: Boolean)

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
    avroSchema.getType match {
      case INT => avroSchema.getLogicalType match {
        case _: Date => SchemaType(DateType, nullable = false)
        case _ => SchemaType(IntegerType, nullable = false)
      }
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES | FIXED => avroSchema.getLogicalType match {
        // For FIXED type, if the precision requires more bytes than fixed size, the logical
        // type will be null, which is handled by Avro library.
        case d: Decimal => SchemaType(DecimalType(d.getPrecision, d.getScale), nullable = false)
        case _ => SchemaType(BinaryType, nullable = false)
      }

      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(TimestampType, nullable = false)
        case logicalType if isLocalTimestampMillis(logicalType) || isLocalTimestampMicros(logicalType) =>
          timestampNTZTypeInstance match {
            case Some(timestampNTZ) => SchemaType(timestampNTZ, nullable = false)
            case None => SchemaType(LongType, nullable = false) // Fallback for older Spark versions
          }
        case _ => SchemaType(LongType, nullable = false)
      }

      case ENUM => SchemaType(StringType, nullable = false)

      case NULL => SchemaType(NullType, nullable = true)

      case RECORD =>
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
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields.toSeq), nullable = false)

      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
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
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                // All fields are nullable because only one of them is set at a time
                StructField(s"$unionFieldMemberPrefix$i", schemaType.dataType, nullable = true)
            }

            SchemaType(StructType(fields.toSeq), nullable = false)
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
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())
      case dataType if isTimestampNTZType(dataType) =>
        createLocalTimestampMicrosSchema()

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType | CharType(_) | VarcharType(_) => builder.stringType()
      case NullType => builder.nullType()
      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case BinaryType => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        if (canBeUnion(st)) {
          val nonNullUnionFieldTypes = st.map(f => toAvroType(f.dataType, nullable = false, f.name, childNameSpace))
          val unionFieldTypes = if (nullable) {
            nullSchema +: nonNullUnionFieldTypes
          } else {
            nonNullUnionFieldTypes
          }
          Schema.createUnion(unionFieldTypes:_*)
        } else {
          val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
          st.foreach { f =>
            val fieldAvroType =
              toAvroType(f.dataType, f.nullable, f.name, childNameSpace)
            fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
          }
          fieldsAssembler.endRecord()
        }

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }

    if (nullable && catalystType != NullType && schema.getType != Schema.Type.UNION) {
      Schema.createUnion(schema, nullSchema)
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
