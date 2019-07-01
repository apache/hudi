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


package com.uber.hoodie

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.LocalDate

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.SchemaBuilder._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.generic.GenericFixed
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

object SchemaConverters {
  class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  case class SchemaType(dataType: DataType, nullable: Boolean)

  object LogicalTypePredicates {
    type FieldPredicate = Schema => Boolean

    implicit class FieldPredicateOps(pred: FieldPredicate) {
      def and(other: FieldPredicate): FieldPredicate = {
        schema => pred(schema) && other(schema)
      }
    }

    def withProp[T: ClassTag](prop: (String, T)): FieldPredicate = schema => {
      val classTag = implicitly[ClassTag[T]]
      schema.getProps.asScala.get(prop._1)
        .exists(v => classTag.unapply(v).isDefined && v.asInstanceOf[T] == prop._2)
    }

    def ofSchema(aType: Schema.Type): FieldPredicate = schema =>
      schema.getType == aType

    val DECIMAL: FieldPredicate = ofSchema(FIXED) and withProp("logicalType" -> "decimal")

    val TIMESTAMP: FieldPredicate = ofSchema(LONG) and withProp("logicalType" -> "timestamp-millis")

    val DATE: FieldPredicate = ofSchema(INT) and withProp("logicalType" -> "date")
  }

  /**
    * This function takes an avro schema and returns a sql schema.
    */
  def toSqlType(avroSchema: Schema): SchemaType = {
    toSqlTypeHelper(avroSchema, Set.empty)
  }

  def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {
    avroSchema.getType match {
      case INT => avroSchema.getLogicalType match {
        case _: LogicalTypes.Date => SchemaType(DateType, nullable = false)
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
        case _ => SchemaType(LongType, nullable = false)
      }

      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
                                                   |Found recursive reference in Avro schema, which can not be processed by Spark:
                                                   |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

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
        } else avroSchema.getTypes.asScala.map(_.getType) match {
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
                StructField(s"member$i", schemaType.dataType, nullable = true)
            }

            SchemaType(StructType(fields), nullable = false)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  /**
    * This function converts sparkSQL StructType into avro schema. This method uses two other
    * converter methods in order to do the conversion.
    */
  def convertStructToAvro[T](structType: StructType,
                             schemaBuilder: RecordBuilder[T],
                             recordNamespace: SchemaNsNaming): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields.foreach { field =>
      val fieldName = field.name
      val schemaBuilder = getSchemaBuilder(field.nullable)
      val fieldSchema = convertTypeToAvro(field.dataType, schemaBuilder, fieldName,
        recordNamespace.structFieldNaming(fieldName))

      fieldsAssembler.name(fieldName).`type`(fieldSchema).noDefault()
    }
    fieldsAssembler.endRecord()
  }

  /**
    * Set up a method to keep the external API unchanged
    */
  def convertStructToAvro[T](structType: StructType, schemaBuilder: RecordBuilder[T],
                             recordNamespace: String): T = {
    convertStructToAvro(structType, schemaBuilder, SchemaNsNaming.fromName(recordNamespace))
  }

  /**
    * Returns a converter function to convert row in avro format to GenericRow of catalyst.
    *
    * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
    *                       by user.
    * @param targetSqlType Target catalyst sql type after the conversion.
    * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
    */
  def createConverterToSQL(sourceAvroSchema: Schema, targetSqlType: DataType): AnyRef => AnyRef = {

    def createConverter(avroSchema: Schema,
                        sqlType: DataType, path: List[String]): AnyRef => AnyRef = {
      val logicalTypeConverters: PartialFunction[(DataType, Schema), AnyRef => AnyRef] = {
        case (d: DecimalType, s) if LogicalTypePredicates.DECIMAL.apply(s) =>
          item => Option(item.asInstanceOf[ByteBuffer].array())
            .map(arr => new java.math.BigDecimal(new java.math.BigInteger(arr), d.scale))
            .orNull

        case (_: TimestampType, s) if LogicalTypePredicates.TIMESTAMP.apply(s) =>
          item => new Timestamp(item.asInstanceOf[Long])

        case (_: DateType, s) if LogicalTypePredicates.DATE.apply(s) =>
          item => java.sql.Date.valueOf(LocalDate.ofEpochDay(item.asInstanceOf[Int]))
      }

      val fallbackConverters: PartialFunction[(DataType, Schema.Type), AnyRef => AnyRef] = {
        // Avro strings are in Utf8, so we have to call toString on them
        case (StringType, STRING) | (StringType, ENUM) =>
          (item: AnyRef) => item.toString
        // Byte arrays are reused by avro, so we have to make a copy of them.
        case (IntegerType, INT) | (BooleanType, BOOLEAN) | (DoubleType, DOUBLE) |
             (FloatType, FLOAT) | (LongType, LONG) =>
          identity
        case (TimestampType, LONG) =>
          (item: AnyRef) => new Timestamp(item.asInstanceOf[Long])
        case (DateType, LONG) =>
          (item: AnyRef) => new java.sql.Date(item.asInstanceOf[Long])
        case (BinaryType, FIXED) =>
          (item: AnyRef) => item.asInstanceOf[GenericFixed].bytes().clone()
        case (BinaryType, BYTES) =>
          (item: AnyRef) =>
            val byteBuffer = item.asInstanceOf[ByteBuffer]
            val bytes = new Array[Byte](byteBuffer.remaining)
            byteBuffer.get(bytes)
            bytes
        case (struct: StructType, RECORD) =>
          val length = struct.fields.length
          val converters = new Array[AnyRef => AnyRef](length)
          val avroFieldIndexes = new Array[Int](length)
          var i = 0
          while (i < length) {
            val sqlField = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = (item: AnyRef) => {
                if (item == null) {
                  item
                } else {
                  createConverter(avroField.schema, sqlField.dataType, path :+ sqlField.name)(item)
                }
              }
              converters(i) = converter
              avroFieldIndexes(i) = avroField.pos()
            } else if (!sqlField.nullable) {
              throw new IncompatibleSchemaException(
                s"Cannot find non-nullable field ${sqlField.name} at path ${path.mkString(".")} " +
                  "in Avro schema\n" +
                  s"Source Avro schema: $sourceAvroSchema.\n" +
                  s"Target Catalyst type: $targetSqlType")
            }
            i += 1
          }

          (item: AnyRef) =>
            val record = item.asInstanceOf[GenericRecord]
            val result = new Array[Any](length)
            var i = 0
            while (i < converters.length) {
              if (converters(i) != null) {
                val converter = converters(i)
                result(i) = converter(record.get(avroFieldIndexes(i)))
              }
              i += 1
            }
            new GenericRow(result)
        case (arrayType: ArrayType, ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType,
            path)
          val allowsNull = arrayType.containsNull
          (item: AnyRef) =>
            item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
              if (element == null && !allowsNull) {
                throw new RuntimeException(s"Array value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                elementConverter(element)
              }
            }
        case (mapType: MapType, MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          val allowsNull = mapType.valueContainsNull
          (item: AnyRef) =>
            item.asInstanceOf[java.util.Map[AnyRef, AnyRef]].asScala.map { case (k, v) =>
              if (v == null && !allowsNull) {
                throw new RuntimeException(s"Map value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                (k.toString, valueConverter(v))
              }
            }.toMap
        case (sqlType, UNION) =>
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (remainingUnionTypes.size == 1) {
              createConverter(remainingUnionTypes.head, sqlType, path)
            } else {
              createConverter(Schema.createUnion(remainingUnionTypes.asJava), sqlType, path)
            }
          } else avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) => createConverter(avroSchema.getTypes.get(0), sqlType, path)
            case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sqlType == LongType =>
              (item: AnyRef) =>
                item match {
                  case l: java.lang.Long => l
                  case i: java.lang.Integer => new java.lang.Long(i.longValue())
                }
            case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && sqlType == DoubleType =>
              (item: AnyRef) =>
                item match {
                  case d: java.lang.Double => d
                  case f: java.lang.Float => new java.lang.Double(f.doubleValue())
                }
            case other =>
              sqlType match {
                case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                  val fieldConverters = t.fields.zip(avroSchema.getTypes.asScala).map {
                    case (field, schema) =>
                      createConverter(schema, field.dataType, path :+ field.name)
                  }
                  (item: AnyRef) =>
                    val i = GenericData.get().resolveUnion(avroSchema, item)
                    val converted = new Array[Any](fieldConverters.length)
                    converted(i) = fieldConverters(i)(item)
                    new GenericRow(converted)
                case _ => throw new IncompatibleSchemaException(
                  s"Cannot convert Avro schema to catalyst type because schema at path " +
                    s"${path.mkString(".")} is not compatible " +
                    s"(avroType = $other, sqlType = $sqlType). \n" +
                    s"Source Avro schema: $sourceAvroSchema.\n" +
                    s"Target Catalyst type: $targetSqlType")
              }
          }
      }
      val wrappedFallbackConverters: PartialFunction[(DataType, Schema), AnyRef => AnyRef] = {
        case (dt, s) if fallbackConverters.isDefinedAt(dt, s.getType) =>
          fallbackConverters.apply(dt, s.getType)
      }
      (logicalTypeConverters orElse wrappedFallbackConverters)
        .applyOrElse((sqlType, avroSchema), { unknown: (DataType, Schema) =>
          val (left, right) = unknown
          throw new IncompatibleSchemaException(
            s"Cannot convert Avro schema to catalyst type because schema at path " +
              s"${path.mkString(".")} is not compatible " +
              s"(avroType = ${right.getType}, sqlType = $left). \n" +
              s"Source Avro schema: $sourceAvroSchema.\n" +
              s"Target Catalyst type: $targetSqlType")
        })
    }
    createConverter(sourceAvroSchema, targetSqlType, List.empty[String])
  }

  /**
    * This function is used to convert some sparkSQL type to avro type.
    */
  private def convertTypeToAvro(
                                 dataType: DataType,
                                 schemaBuilder: BaseTypeBuilder[Schema],
                                 structName: String,
                                 recordNamespace: SchemaNsNaming): Schema = {
    dataType match {
      case ByteType => schemaBuilder.intType()
      case ShortType => schemaBuilder.intType()
      case IntegerType => schemaBuilder.intType()
      case LongType => schemaBuilder.longType()
      case FloatType => schemaBuilder.floatType()
      case DoubleType => schemaBuilder.doubleType()
      case dec: DecimalType =>
        val avroType = LogicalTypes.decimal(dec.precision, dec.scale)
        val fixedSize = minBytesForPrecision(dec.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = recordNamespace.currentNamespace
        val decimalSchema = avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))
        Schema.createUnion(decimalSchema, nullSchema)
      case StringType => schemaBuilder.stringType()
      case BinaryType => schemaBuilder.bytesType()
      case BooleanType => schemaBuilder.booleanType()
      case TimestampType =>
        val logicalSchema = LogicalTypes.timestampMillis()
          .addToSchema(SchemaBuilder.builder().longType())
        schemaBuilder.`type`(logicalSchema)
      case DateType =>
        val logicalSchema = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())
        schemaBuilder.`type`(logicalSchema)

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName,
          recordNamespace.arrayFieldNaming(structName, elementType))
        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName,
          recordNamespace.mapFieldNaming(structName, valueType))
        schemaBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          schemaBuilder.record(structName).namespace(recordNamespace.currentNamespace),
          recordNamespace)

      case other => throw new IncompatibleSchemaException(s"Unexpected type $dataType.")
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] = {
    if (isNullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }
  }

  lazy val minBytesForPrecision = Array.tabulate[Int](39)(computeMinBytesForPrecision)

  private def computeMinBytesForPrecision(precision : Int) : Int = {
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }
}
