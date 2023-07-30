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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.{Locale, TimeZone, Map => JMap}
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema._
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.parquet.Spark24ParquetReadSupport.getSchemaConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A Parquet [[ReadSupport]] implementation for reading Parquet records as Catalyst
 * [[UnsafeRow]]s.
 *
 * The API interface of [[ReadSupport]] is a little bit over complicated because of historical
 * reasons.  In older versions of parquet-mr (say 1.6.0rc3 and prior), [[ReadSupport]] need to be
 * instantiated and initialized twice on both driver side and executor side.  The [[init()]] method
 * is for driver side initialization, while [[prepareForRead()]] is for executor side.  However,
 * starting from parquet-mr 1.6.0, it's no longer the case, and [[ReadSupport]] is only instantiated
 * and initialized on executor side.  So, theoretically, now it's totally fine to combine these two
 * methods into a single initialization method.  The only reason (I could think of) to still have
 * them here is for parquet-mr API backwards-compatibility.
 *
 * Due to this reason, we no longer rely on [[ReadContext]] to pass requested schema from [[init()]]
 * to [[prepareForRead()]], but use a private `var` for simplicity.
 */
private[parquet] class Spark24ParquetReadSupport(val convertTz: Option[TimeZone], readerType: String)
  extends ReadSupport[UnsafeRow] with Logging {
  private var catalystRequestedSchema: StructType = _

  def this(readerType: String) {
    // We need a zero-arg constructor for SpecificParquetRecordReaderBase.  But that is only
    // used in the vectorized reader, where we get the convertTz value directly, and the value here
    // is ignored.
    this(None, readerType)
  }

  /**
   * Called on executor side before [[prepareForRead()]] and instantiating actual Parquet record
   * readers.  Responsible for figuring out Parquet requested schema used for column pruning.
   */
  override def init(context: InitContext): ReadContext = {
    catalystRequestedSchema = {
      val conf = context.getConfiguration
      val schemaString = conf.get(getSchemaConfig(readerType))
      assert(schemaString != null, "Parquet requested schema not set.")
      StructType.fromString(schemaString)
    }

    val caseSensitive = context.getConfiguration.getBoolean(SQLConf.CASE_SENSITIVE.key,
      SQLConf.CASE_SENSITIVE.defaultValue.get)
    val parquetRequestedSchema = Spark24ParquetReadSupport.clipParquetSchema(
      context.getFileSchema, catalystRequestedSchema, caseSensitive)

    new ReadContext(parquetRequestedSchema, Map.empty[String, String].asJava)
  }

  /**
   * Called on executor side after [[init()]], before instantiating actual Parquet record readers.
   * Responsible for instantiating [[RecordMaterializer]], which is used for converting Parquet
   * records to Catalyst [[UnsafeRow]]s.
   */
  override def prepareForRead(
                               conf: Configuration,
                               keyValueMetaData: JMap[String, String],
                               fileSchema: MessageType,
                               readContext: ReadContext): RecordMaterializer[UnsafeRow] = {
    log.debug(s"Preparing for read Parquet file with message type: $fileSchema")
    val parquetRequestedSchema = readContext.getRequestedSchema

    logInfo {
      s"""Going to read the following fields from the Parquet file:
         |
         |Parquet form:
         |$parquetRequestedSchema
         |Catalyst form:
         |$catalystRequestedSchema
       """.stripMargin
    }

    new ParquetRecordMaterializer(
      parquetRequestedSchema,
      Spark24ParquetReadSupport.expandUDT(catalystRequestedSchema),
      new ParquetToSparkSchemaConverter(conf),
      convertTz)
  }
}

private[parquet] object Spark24ParquetReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"
  val SPARK_ROW_REQUESTED_SCHEMA_MOR = "org.apache.spark.sql.parquet.row.requested_schema.mor"
  val SPARK_ROW_REQUESTED_SCHEMA_SKELETON = "org.apache.spark.sql.parquet.row.requested_schema.skeleton"
  val SPARK_ROW_REQUESTED_SCHEMA_BOOTSTRAP = "org.apache.spark.sql.parquet.row.requested_schema.bootstrap"


  def getSchemaConfig(readerType: String): String = {
    readerType match {
      case "mor" => SPARK_ROW_REQUESTED_SCHEMA_MOR
      case "skeleton" => SPARK_ROW_REQUESTED_SCHEMA_SKELETON
      case "bootstrap" => SPARK_ROW_REQUESTED_SCHEMA_BOOTSTRAP
      case _ => SPARK_ROW_REQUESTED_SCHEMA
    }
  }

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  /**
   * Tailors `parquetSchema` according to `catalystSchema` by removing column paths don't exist
   * in `catalystSchema`, and adding those only exist in `catalystSchema`.
   */
  def clipParquetSchema(
                         parquetSchema: MessageType,
                         catalystSchema: StructType,
                         caseSensitive: Boolean = true): MessageType = {
    val clippedParquetFields = clipParquetGroupFields(
      parquetSchema.asGroupType(), catalystSchema, caseSensitive)
    if (clippedParquetFields.isEmpty) {
      ParquetSchemaConverter.EMPTY_MESSAGE
    } else {
      Types
        .buildMessage()
        .addFields(clippedParquetFields: _*)
        .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
    }
  }

  private def clipParquetType(
                               parquetType: Type, catalystType: DataType, caseSensitive: Boolean): Type = {
    catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        clipParquetListType(parquetType.asGroupType(), t.elementType, caseSensitive)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
          !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        clipParquetMapType(parquetType.asGroupType(), t.keyType, t.valueType, caseSensitive)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t, caseSensitive)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        parquetType
    }
  }

  /**
   * Whether a Catalyst [[DataType]] is primitive.  Primitive [[DataType]] is not equivalent to
   * [[AtomicType]].  For example, [[CalendarIntervalType]] is primitive, but it's not an
   * [[AtomicType]].
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[ArrayType]].  The element type
   * of the [[ArrayType]] should also be a nested type, namely an [[ArrayType]], a [[MapType]], or a
   * [[StructType]].
   */
  private def clipParquetListType(
                                   parquetList: GroupType, elementType: DataType, caseSensitive: Boolean): Type = {
    // Precondition of this method, should only be called for lists with nested element types.
    assert(!isPrimitiveCatalystType(elementType))

    // Unannotated repeated group should be interpreted as required list of required element, so
    // list element type is just the group itself.  Clip it.
    if (parquetList.getOriginalType == null && parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType, caseSensitive)
    } else {
      assert(
        parquetList.getOriginalType == OriginalType.LIST,
        "Invalid Parquet schema. " +
          "Original type of annotated Parquet lists must be LIST: " +
          parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
          "LIST-annotated group should only have exactly one repeated field: " +
          parquetList)

      // Precondition of this method, should only be called for lists with nested element types.
      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()

      // If the repeated field is a group with multiple fields, or the repeated field is a group
      // with one field and is named either "array" or uses the LIST-annotated group's name with
      // "_tuple" appended then the repeated type is the element type and elements are required.
      // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
      // only field.
      if (
        repeatedGroup.getFieldCount > 1 ||
          repeatedGroup.getName == "array" ||
          repeatedGroup.getName == parquetList.getName + "_tuple"
      ) {
        Types
          .buildGroup(parquetList.getRepetition)
          .as(OriginalType.LIST)
          .addField(clipParquetType(repeatedGroup, elementType, caseSensitive))
          .named(parquetList.getName)
      } else {
        // Otherwise, the repeated field's type is the element type with the repeated field's
        // repetition.
        Types
          .buildGroup(parquetList.getRepetition)
          .as(OriginalType.LIST)
          .addField(
            Types
              .repeatedGroup()
              .addField(clipParquetType(repeatedGroup.getType(0), elementType, caseSensitive))
              .named(repeatedGroup.getName))
          .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[MapType]].  Either key type or
   * value type of the [[MapType]] must be a nested type, namely an [[ArrayType]], a [[MapType]], or
   * a [[StructType]].
   */
  private def clipParquetMapType(
                                  parquetMap: GroupType,
                                  keyType: DataType,
                                  valueType: DataType,
                                  caseSensitive: Boolean): GroupType = {
    // Precondition of this method, only handles maps with nested key types or value types.
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup =
      Types
        .repeatedGroup()
        .as(repeatedGroup.getOriginalType)
        .addField(clipParquetType(parquetKeyType, keyType, caseSensitive))
        .addField(clipParquetType(parquetValueType, valueType, caseSensitive))
        .named(repeatedGroup.getName)

    Types
      .buildGroup(parquetMap.getRepetition)
      .as(parquetMap.getOriginalType)
      .addField(clippedRepeatedGroup)
      .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A clipped [[GroupType]], which has at least one field.
   * @note Parquet doesn't allow creating empty [[GroupType]] instances except for empty
   *       [[MessageType]].  Because it's legal to construct an empty requested schema for column
   *       pruning.
   */
  private def clipParquetGroup(
                                parquetRecord: GroupType, structType: StructType, caseSensitive: Boolean): GroupType = {
    val clippedParquetFields = clipParquetGroupFields(parquetRecord, structType, caseSensitive)
    Types
      .buildGroup(parquetRecord.getRepetition)
      .as(parquetRecord.getOriginalType)
      .addFields(clippedParquetFields: _*)
      .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A list of clipped [[GroupType]] fields, which can be empty.
   */
  private def clipParquetGroupFields(
                                      parquetRecord: GroupType, structType: StructType, caseSensitive: Boolean): Seq[Type] = {
    val toParquet = new SparkToParquetSchemaConverter(writeLegacyParquetFormat = false)
    if (caseSensitive) {
      val caseSensitiveParquetFieldMap =
        parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
      structType.map { f =>
        caseSensitiveParquetFieldMap
          .get(f.name)
          .map(clipParquetType(_, f.dataType, caseSensitive))
          .getOrElse(toParquet.convertField(f))
      }
    } else {
      // Do case-insensitive resolution only if in case-insensitive mode
      val caseInsensitiveParquetFieldMap =
        parquetRecord.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
      structType.map { f =>
        caseInsensitiveParquetFieldMap
          .get(f.name.toLowerCase(Locale.ROOT))
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw new RuntimeException(s"""Found duplicate field(s) "${f.name}": """ +
                s"$parquetTypesString in case-insensitive mode")
            } else {
              clipParquetType(parquetTypes.head, f.dataType, caseSensitive)
            }
          }.getOrElse(toParquet.convertField(f))
      }
    }
  }

  def expandUDT(schema: StructType): StructType = {
    def expand(dataType: DataType): DataType = {
      dataType match {
        case t: ArrayType =>
          t.copy(elementType = expand(t.elementType))

        case t: MapType =>
          t.copy(
            keyType = expand(t.keyType),
            valueType = expand(t.valueType))

        case t: StructType =>
          val expandedFields = t.fields.map(f => f.copy(dataType = expand(f.dataType)))
          t.copy(fields = expandedFields)

        case t: UserDefinedType[_] =>
          t.sqlType

        case t =>
          t
      }
    }

    expand(schema).asInstanceOf[StructType]
  }
}
