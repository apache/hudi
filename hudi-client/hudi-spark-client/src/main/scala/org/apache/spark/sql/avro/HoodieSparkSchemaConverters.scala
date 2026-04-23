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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.schema.{HoodieJsonProperties, HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.common.schema.HoodieSchema.TimePrecision
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal.minBytesForPrecision

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * Object containing methods to convert HoodieSchema to Spark SQL schemas and vice versa.
 *
 * This provides direct conversion between HoodieSchema and Spark DataType
 * without going through Avro Schema intermediary.
 *
 * Version-specific types (like VariantType in Spark >4.x) are handled via SparkAdapterSupport.
 *
 * NOTE: the package of this class is intentionally kept as "org.apache.spark.sql.avro" which is similar to the existing
 * Spark Avro connector's SchemaConverters.scala
 * (https://github.com/apache/spark/blob/master/connector/avro/src/main/scala/org/apache/spark/sql/avro/SchemaConverters.scala).
 * The reason for this is so that Spark 3.3 is able to access private spark sql type classes like TimestampNTZType.
 */
@DeveloperApi
object HoodieSparkSchemaConverters extends SparkAdapterSupport {

  /**
   * Internal wrapper for SQL data type and nullability.
   */
  case class SchemaType(dataType: DataType, nullable: Boolean, metadata: Option[Metadata] = None)

  def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean) = {
    val result = toSqlTypeHelper(hoodieSchema, Set.empty)
    (result.dataType, result.nullable)
  }

  def toHoodieType(catalystType: DataType,
                   nullable: Boolean = false,
                   recordName: String = "topLevelRecord",
                   nameSpace: String = "",
                   metadata: Metadata = Metadata.empty): HoodieSchema = {
    toHoodieTypeNested(catalystType, nullable, recordName, nameSpace, metadata, depth = 0)
  }

  /**
   * Walks a user-supplied StructType and rejects any field tagged with
   * {@code hudi_type=BLOB} or {@code hudi_type=VARIANT} whose inner Spark shape does not
   * match the canonical layout.
   *
   * Intended for the ingest/write boundary only. {@link #toHoodieTypeNested} is shared
   * with the read/prune path and must stay permissive so that Spark's nested-schema
   * pruning (which can strip sibling fields while preserving the outer {@code hudi_type}
   * metadata) does not crash projections. Validation is therefore hoisted up to the
   * writer, which always sees the user's full, unpruned schema.
   *
   * @throws IllegalArgumentException if any tagged field has a non-canonical inner shape
   */
  def validateCustomTypeStructures(structType: StructType): Unit =
    validateCustomTypeStructuresRecursive(structType)

  private def validateCustomTypeStructuresRecursive(dataType: DataType): Unit = dataType match {
    case s: StructType =>
      s.fields.foreach { f =>
        if (f.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
          val descriptorType = HoodieSchema
            .parseTypeDescriptor(f.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
            .getType
          (descriptorType, f.dataType) match {
            case (HoodieSchemaType.BLOB, st: StructType) => validateBlobStructure(st)
            case (HoodieSchemaType.VARIANT, st: StructType) => validateVariantStructure(st)
            case _ =>
          }
        }
        validateCustomTypeStructuresRecursive(f.dataType)
      }
    case ArrayType(elementType, _) => validateCustomTypeStructuresRecursive(elementType)
    case MapType(_, valueType, _) => validateCustomTypeStructuresRecursive(valueType)
    case _ =>
  }

  /**
   * Converts a Spark DataType to a HoodieSchema, tracking how deeply nested the current type is
   * relative to the top-level table schema. This depth is used to enforce that VECTOR columns can
   * only appear as direct fields of the root record — not inside nested structs, arrays, or maps.
   *
   * The caller passes depth=0 for the root StructType. Each level of nesting increments depth by 1,
   * so direct fields of the root record are at depth=1 (VECTOR allowed), and anything deeper is
   * at depth≥2 (VECTOR not allowed).
   */
  private def toHoodieTypeNested(catalystType: DataType,
                                 nullable: Boolean,
                                 recordName: String,
                                 nameSpace: String,
                                 metadata: Metadata,
                                 depth: Int): HoodieSchema = {
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
      case ArrayType(elementSparkType, containsNull)
        if metadata.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
          HoodieSchema.parseTypeDescriptor(metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).getType == HoodieSchemaType.VECTOR =>
        if (depth > 1) {
          throw new HoodieSchemaException(
            s"VECTOR column '$recordName' must be a top-level field. Nested VECTOR columns (inside STRUCT, ARRAY, or MAP) are not supported.")
        }
        if (containsNull) {
          throw new HoodieSchemaException(
            s"VECTOR type does not support nullable elements (field: $recordName)")
        }

        val vectorSchema = HoodieSchema
          .parseTypeDescriptor(metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
          .asInstanceOf[HoodieSchema.Vector]
        val dimension = vectorSchema.getDimension

        val elementType = vectorSchema.getVectorElementType

        val expectedSparkType = sparkTypeForVectorElementType(elementType)
        if (elementSparkType != expectedSparkType) {
          throw new HoodieSchemaException(
            s"VECTOR element type mismatch for field $recordName: metadata requires $elementType, Spark array has $elementSparkType")
        }

        HoodieSchema.createVector(dimension, elementType)

      case ArrayType(elementType, containsNull) =>
        val elementSchema = toHoodieTypeNested(elementType, containsNull, recordName, nameSpace, metadata, depth + 1)
        HoodieSchema.createArray(elementSchema)

      case MapType(StringType, valueType, valueContainsNull) =>
        val valueSchema = toHoodieTypeNested(valueType, valueContainsNull, recordName, nameSpace, metadata, depth + 1)
        HoodieSchema.createMap(valueSchema)

      case blobStruct: StructType if metadata.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
        HoodieSchema.parseTypeDescriptor(metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).getType == HoodieSchemaType.BLOB &&
        isCanonicalBlobStruct(blobStruct) =>
        // Canonical RFC-100 BLOB layout. Pruned BLOB structs (Spark 3.3/3.4 keeps the
        // hudi_type=BLOB metadata on the outer StructField while dropping sibling inner
        // fields during nested schema pruning) fall through to the plain RECORD branch
        // below; HoodieSchemaUtils.pruneDataSchema then restores the full BLOB from the
        // data schema.
        HoodieSchema.createBlob()

      case variantStruct: StructType if metadata.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
        HoodieSchema.parseTypeDescriptor(metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).getType == HoodieSchemaType.VARIANT &&
        isCanonicalVariantStruct(variantStruct) =>
        HoodieSchema.createVariant(recordName, nameSpace, null)

      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName

        // Check if this might be a union (using heuristic like Avro converter)
        if (canBeUnion(st)) {
          val nonNullUnionFieldTypes = st.map { f =>
            toHoodieTypeNested(f.dataType, nullable = false, f.name, childNameSpace, f.metadata, depth + 1)
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
            val fieldSchema = toHoodieTypeNested(f.dataType, f.nullable, f.name, childNameSpace, f.metadata, depth + 1)
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

      // VARIANT type (Spark >4.x only), which will be handled via SparkAdapter
      case other if sparkAdapter.isVariantType(other) =>
        HoodieSchema.createVariant(recordName, nameSpace, null)

      case other =>
        throw new IncompatibleSchemaException(s"Unexpected Spark DataType: $other")
    }

    // Wrap with null union if nullable (and not already a union)
    if (nullable && catalystType != NullType && schema.getType != HoodieSchemaType.UNION) {
      HoodieSchema.createNullable(schema)
    } else {
      schema
    }
  }

  /**
   * Helper method to convert HoodieSchema to Catalyst DataType.
   */
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
      case HoodieSchemaType.VECTOR =>
        val vectorSchema = hoodieSchema.asInstanceOf[HoodieSchema.Vector]
        val metadata = new MetadataBuilder()
          .putString(HoodieSchema.TYPE_METADATA_FIELD, vectorSchema.toTypeDescriptor)
          .build()

        val sparkElementType = sparkTypeForVectorElementType(vectorSchema.getVectorElementType)
        SchemaType(ArrayType(sparkElementType, containsNull = false), nullable = false, Some(metadata))

      case HoodieSchemaType.BLOB | HoodieSchemaType.RECORD =>
        val isBlob = hoodieSchema.getType == HoodieSchemaType.BLOB
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
          val fieldSchema = f.getNonNullSchema
          val metadataBuilder = new MetadataBuilder()
            .withMetadata(schemaType.metadata.getOrElse(Metadata.empty))
          if (f.doc().isPresent && f.doc().get().nonEmpty) {
            metadataBuilder.putString("comment", f.doc().get())
          }
          if (fieldSchema.isBlobField) {
            metadataBuilder.putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchema.Blob.TYPE_DESCRIPTOR)
          }
          val metadata = metadataBuilder.build()
          // For BLOB: force nullable-everywhere at the Spark type layer. The RFC-100
          // canonical schema declares `type`, `reference.external_path`, and
          // `reference.managed` as strictly non-null, but that contract is conditional
          // ("required when parent is present") and Spark's type system can't model it.
          // Projecting BLOB as nullable-everywhere for Spark avoids downstream pain
          // (Cast / TableOutputResolver / Cast.canCast rewrites); the on-disk physical
          // schema stays RFC-100 compliant because the write path goes through
          // HoodieSchema.Blob.createBlob(), which uses the canonical fields verbatim.
          if (isBlob) {
            StructField(f.name(), withAllFieldsNullable(schemaType.dataType), nullable = true, metadata)
          } else {
            StructField(f.name(), schemaType.dataType, schemaType.nullable, metadata)
          }
        }
        // For BLOB types, propagate type metadata via SchemaType
        val schemaTypeMetadata = if (isBlob) {
          Some(new MetadataBuilder()
            .putString(HoodieSchema.TYPE_METADATA_FIELD, hoodieSchema.asInstanceOf[HoodieSchema.Blob].toTypeDescriptor)
            .build())
        } else {
          None
        }
        SchemaType(StructType(fields.toSeq), nullable = false, schemaTypeMetadata)

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

      // VARIANT type (Spark >4.x only), which will be handled via SparkAdapter
      // TODO: Check if internalSchema will throw any errors here: #18021
      case HoodieSchemaType.VARIANT =>
        sparkAdapter.getVariantDataType match {
          case Some(variantType) => SchemaType(variantType, nullable = false)
          case None => throw new IncompatibleSchemaException("VARIANT type is only supported in Spark 4.0+")
        }

      case other =>
        throw new IncompatibleSchemaException(s"Unsupported HoodieSchemaType: $other")
    }
  }

  private lazy val expectedBlobStructType: StructType = toSqlType(HoodieSchema.createBlob())._1.asInstanceOf[StructType]

  /**
   * Validates that a StructType matches the expected blob schema structure defined in {@link HoodieSchema.Blob}.
   *
   * Purely structural: compares field names and data types recursively, ignoring nullability.
   * At the Spark type layer, BLOB is projected as nullable-everywhere by [[toSqlType]] (see the
   * comment on the BLOB case there); nullability is therefore not part of the structural
   * contract. The RFC-100 non-null invariants are enforced at the physical-schema write
   * boundary by {@link HoodieSchema.Blob#createBlob}.
   *
   * @param structType the StructType to validate
   * @throws IllegalArgumentException if the structure does not match the expected blob schema
   */
  private def validateBlobStructure(structType: StructType): Unit = {
    if (!isCanonicalBlobStruct(structType)) {
      throw new IllegalArgumentException(
        s"""Invalid blob schema structure. Expected schema:
           |${expectedBlobStructType.toDDL}
           |Got schema:
           |${structType.toDDL}""".stripMargin)
    }
  }

  /**
   * Returns true if the StructType matches the canonical RFC-100 BLOB layout.
   * Used both by the write/ingest validator and to distinguish a genuine BLOB
   * struct from one that Spark's nested-schema pruning has partially stripped
   * (Spark 3.3/3.4 preserves the hudi_type=BLOB metadata on the outer field
   * while dropping sibling inner fields).
   */
  private def isCanonicalBlobStruct(structType: StructType): Boolean =
    matchesStructure(structType, expectedBlobStructType, SQLConf.get.caseSensitiveAnalysis)

  private def matchesStructure(source: DataType, expected: DataType, caseSensitive: Boolean): Boolean =
    (source, expected) match {
      case (s: StructType, e: StructType) =>
        s.length == e.length && s.fields.zip(e.fields).forall { case (sf, ef) =>
          nameEquals(sf.name, ef.name, caseSensitive) &&
            matchesStructure(sf.dataType, ef.dataType, caseSensitive)
        }
      case _ => source == expected
    }

  private def nameEquals(a: String, b: String, caseSensitive: Boolean): Boolean =
    if (caseSensitive) a == b else a.equalsIgnoreCase(b)

  private def withAllFieldsNullable(dataType: DataType): DataType = dataType match {
    case s: StructType =>
      StructType(s.fields.map(f => f.copy(
        dataType = withAllFieldsNullable(f.dataType),
        nullable = true)))
    case ArrayType(elementType, _) =>
      ArrayType(withAllFieldsNullable(elementType), containsNull = true)
    case MapType(keyType, valueType, _) =>
      MapType(keyType, withAllFieldsNullable(valueType), valueContainsNull = true)
    case other => other
  }

  private lazy val expectedVariantStructType: StructType = {
    val metadataField = StructField(HoodieSchema.Variant.VARIANT_METADATA_FIELD, BinaryType, nullable = false)
    val valueField = StructField(HoodieSchema.Variant.VARIANT_VALUE_FIELD, BinaryType, nullable = false)
    StructType(Seq(metadataField, valueField))
  }

  /**
   * Validates that a StructType matches the expected unshredded variant schema
   * (two non-null {@code BinaryType} fields: {@code metadata} and {@code value}).
   *
   * Note on nullability: unlike BLOB, VARIANT is not projected nullable-everywhere at the
   * Spark type layer because the user-facing path is gated. Spark 3.x rejects VARIANT at
   * schema resolution, and Spark 4.0+ exposes it as the native {@code VariantType} populated
   * via {@code parse_json(...)}, never a user-supplied {@code named_struct}. The internal
   * physical layout ({@code struct<metadata, value>} with non-null fields) only appears
   * through {@link HoodieSparkSchemaConverters#toSqlType}, which produces the canonical
   * non-null shape this validator expects.
   *
   * @param structType the StructType to validate
   * @throws IllegalArgumentException if the structure does not match the expected variant schema
   */
  private def validateVariantStructure(structType: StructType): Unit = {
    if (!isCanonicalVariantStruct(structType)) {
      throw new IllegalArgumentException(
        s"""Invalid variant schema structure. Expected schema:
           |${expectedVariantStructType.toDDL}
           |Got schema:
           |${structType.toDDL}""".stripMargin)
    }
  }

  /**
   * Returns true if the StructType matches the canonical unshredded VARIANT layout
   * (two non-null {@code BinaryType} fields: {@code metadata} and {@code value}).
   * Used both by the write/ingest validator and to distinguish a genuine VARIANT
   * struct from one that Spark's nested-schema pruning has partially stripped.
   */
  private def isCanonicalVariantStruct(structType: StructType): Boolean = {
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    val key: String => String =
      if (caseSensitive) identity else (_: String).toLowerCase(Locale.ROOT)
    val fieldsByName = structType.fields.map(f => key(f.name) -> f).toMap
    structType.length == 2 &&
      fieldsByName.get(key(HoodieSchema.Variant.VARIANT_METADATA_FIELD)).exists(f => f.dataType == BinaryType && !f.nullable) &&
      fieldsByName.get(key(HoodieSchema.Variant.VARIANT_VALUE_FIELD)).exists(f => f.dataType == BinaryType && !f.nullable)
  }

  private def canBeUnion(st: StructType): Boolean = {
    st.fields.length > 0 &&
      st.forall { f =>
        f.name.matches("member\\d+") && f.nullable
      }
  }

  private def sparkTypeForVectorElementType(
                                             elementType: HoodieSchema.Vector.VectorElementType): DataType = elementType match {
    case HoodieSchema.Vector.VectorElementType.FLOAT => FloatType
    case HoodieSchema.Vector.VectorElementType.DOUBLE => DoubleType
    case HoodieSchema.Vector.VectorElementType.INT8 => ByteType
  }
}

private[avro] class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
