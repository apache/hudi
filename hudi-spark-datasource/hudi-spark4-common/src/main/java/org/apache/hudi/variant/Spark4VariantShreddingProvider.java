/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.variant;

import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.types.variant.ShreddingUtils;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantSchema;
import org.apache.spark.types.variant.VariantShreddingWriter;
import org.apache.spark.types.variant.VariantShreddingWriter.ShreddedResult;
import org.apache.spark.types.variant.VariantShreddingWriter.ShreddedResultBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.avro.HoodieAvroUtils.unwrapNullable;

/**
 * Implementation of {@link VariantShreddingProvider} using Spark 4's variant parsing library.
 *
 * <p>This class bridges the Avro record path and Spark's {@link VariantShreddingWriter}
 * to allow {@code HoodieRecordType.AVRO} to write shredded variant types. It converts
 * the shredded output into Avro {@link GenericRecord}s that can be written via
 * {@link org.apache.hudi.avro.HoodieAvroWriteSupport}.</p>
 *
 * <p>The shredding logic is delegated to {@link VariantShreddingWriter#castShredded},
 * which handles scalar, object, and array shredding including residual value construction
 * for non-matching fields. This class implements the {@link ShreddedResult} and
 * {@link ShreddedResultBuilder} interfaces to collect the shredded components into
 * Avro GenericRecords.</p>
 */
public class Spark4VariantShreddingProvider implements VariantShreddingProvider {

  // Spark's Decimal.MAX_INT_DIGITS, Decimal.MAX_LONG_DIGITS and DecimalType.MAX_PRECISION.
  private static final int MAX_INT_DIGITS = 9;
  private static final int MAX_LONG_DIGITS = 18;
  private static final int MAX_DECIMAL_PRECISION = 38;

  @Override
  public GenericRecord shredVariantRecord(
      GenericRecord unshreddedVariant,
      Schema shreddedSchema,
      HoodieSchema.Variant variantSchema) {

    ByteBuffer valueBuf = (ByteBuffer) unshreddedVariant.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD);
    ByteBuffer metadataBuf = (ByteBuffer) unshreddedVariant.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD);

    if (valueBuf == null || metadataBuf == null) {
      return null;
    }

    byte[] valueBytes = toByteArray(valueBuf);
    byte[] metadataBytes = toByteArray(metadataBuf);

    Variant variant = new Variant(valueBytes, metadataBytes);

    // Build VariantSchema from the Avro shredded schema, registering
    // Avro schemas at each level for GenericRecord construction.
    AvroShreddedResultBuilder builder = new AvroShreddedResultBuilder();
    VariantSchema sparkSchema = buildVariantSchema(shreddedSchema, true, builder);

    // Delegate to Spark's VariantShreddingWriter for the actual shredding logic.
    AvroShreddedResult result = (AvroShreddedResult)
        VariantShreddingWriter.castShredded(variant, sparkSchema, builder);

    return result.toGenericRecord();
  }

  @Override
  public VariantRebuilder prepareRebuild(String columnPath, GroupType parquetVariantType, Schema unshreddedSchema) {
    Map<VariantSchema, LeafDecoding> decodings = new IdentityHashMap<>();
    VariantSchema sparkSchema = buildReadVariantSchema(parquetVariantType, true, columnPath, columnPath, decodings);
    return shreddedVariant -> rebuildVariantRecord(shreddedVariant, sparkSchema, decodings, unshreddedSchema);
  }

  private static GenericRecord rebuildVariantRecord(GenericRecord shreddedVariant, VariantSchema sparkSchema,
                                                    Map<VariantSchema, LeafDecoding> decodings,
                                                    Schema unshreddedSchema) {
    if (shreddedVariant == null) {
      return null;
    }
    if (shreddedVariant.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD) == null) {
      // metadata is REQUIRED in the shredded schema, so a null here means the base file is malformed;
      // fail fast rather than silently nulling out the variant column.
      throw new HoodieException("Cannot reconstruct variant: shredded record is missing the required '"
          + HoodieSchema.Variant.VARIANT_METADATA_FIELD + "' field; the base file is malformed.");
    }

    // Delegate to Spark's reconstruction algorithm (inverse of castShredded).
    Variant variant = ShreddingUtils.rebuild(new AvroVariantRow(shreddedVariant, sparkSchema, decodings), sparkSchema);

    GenericRecord out = new GenericData.Record(unshreddedSchema);
    out.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(variant.getMetadata()));
    out.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(variant.getValue()));
    return out;
  }

  /**
   * Builds the write-side {@link VariantSchema} from an Avro {@link Schema} representing a
   * shredded variant structure ({@code value}, {@code metadata}, {@code typed_value}).
   *
   * <p>This method also registers the Avro schema mapping in the builder so that
   * {@link AvroShreddedResultBuilder#createEmpty} can create results with the
   * correct Avro schema at each nesting level.</p>
   */
  private VariantSchema buildVariantSchema(Schema avroSchema, boolean isTopLevel,
                                           AvroShreddedResultBuilder builder) {
    Schema.Field valueField = avroSchema.getField(HoodieSchema.Variant.VARIANT_VALUE_FIELD);
    Schema.Field metadataField = avroSchema.getField(HoodieSchema.Variant.VARIANT_METADATA_FIELD);
    Schema.Field typedValueField = avroSchema.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD);

    int fieldCount = 0;
    int variantIdx = valueField != null ? fieldCount++ : -1;
    int topLevelMetadataIdx;
    if (metadataField != null && isTopLevel) {
      topLevelMetadataIdx = fieldCount++;
    } else {
      topLevelMetadataIdx = -1;
      if (metadataField != null) {
        fieldCount++;
      }
    }
    int typedIdx = typedValueField != null ? fieldCount++ : -1;
    int numFields = fieldCount;

    VariantSchema.ScalarType scalarSchema = null;
    VariantSchema.ObjectField[] objectSchema = null;
    VariantSchema arraySchema = null;

    if (typedValueField != null) {
      Schema tvSchema = unwrapNullable(typedValueField.schema());

      switch (tvSchema.getType()) {
        case RECORD:
          // Object shredding: each field has a nested {value, typed_value} sub-struct
          List<VariantSchema.ObjectField> fields = new ArrayList<>();
          for (Schema.Field field : tvSchema.getFields()) {
            Schema fieldSchema = unwrapNullable(field.schema());
            VariantSchema subSchema = buildVariantSchema(fieldSchema, false, builder);
            fields.add(new VariantSchema.ObjectField(field.name(), subSchema));
          }
          objectSchema = fields.toArray(new VariantSchema.ObjectField[0]);
          break;

        case ARRAY:
          // Array shredding: elements follow the shredding schema
          Schema elementSchema = unwrapNullable(tvSchema.getElementType());
          arraySchema = buildVariantSchema(elementSchema, false, builder);
          break;

        default:
          // Scalar shredding. Reject leaf types Spark 4 cannot shred, as
          // SparkShreddingUtils.buildVariantSchema does with invalidVariantShreddingSchema.
          if (!isSparkShreddableScalar(tvSchema)) {
            LogicalType logicalType = tvSchema.getLogicalType();
            String typeName = tvSchema.getType().getName()
                + (logicalType == null ? "" : " (" + logicalType.getName() + ")");
            throw new HoodieException("Variant shredding schema has a typed_value of type " + typeName
                + ", which Spark 4 cannot shred. Supported typed_value types: " + SPARK_SHREDDABLE_SCALAR_TYPES + ".");
          }
          scalarSchema = avroTypeToScalarType(tvSchema);
          break;
      }
    }

    VariantSchema result = new VariantSchema(
        typedIdx, variantIdx, topLevelMetadataIdx, numFields,
        scalarSchema, objectSchema, arraySchema);
    builder.registerSchema(result, avroSchema);
    return result;
  }

  /**
   * The scalar {@code typed_value} types {@link #isSparkShreddableScalar} accepts, for error messages.
   * Keep in sync with that switch.
   */
  private static final String SPARK_SHREDDABLE_SCALAR_TYPES = "boolean, int, long, float, double, string, bytes, "
      + "decimal, date, timestamp-micros, local-timestamp-micros";

  /**
   * Whether a scalar {@code typed_value} is the Avro equivalent of a scalar type Spark 4's
   * {@code SparkShreddingUtils.buildVariantSchema} accepts.
   */
  private static boolean isSparkShreddableScalar(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    String logicalName = logicalType == null ? null : logicalType.getName();
    switch (schema.getType()) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case STRING: // a uuid string is not allowed
        return logicalName == null;
      case INT:
        return logicalName == null || "date".equals(logicalName);
      case LONG:
        return logicalName == null
            || "timestamp-micros".equals(logicalName)
            || "local-timestamp-micros".equals(logicalName);
      case BYTES:
        return logicalName == null || "decimal".equals(logicalName);
      case FIXED:
        return "decimal".equals(logicalName);
      default:
        return false;
    }
  }

  /**
   * Maps a scalar {@code typed_value} Avro {@link Schema} that passed
   * {@link #isSparkShreddableScalar} to a {@link VariantSchema.ScalarType}.
   */
  private VariantSchema.ScalarType avroTypeToScalarType(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();

    // Check logical types first
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
        return new VariantSchema.DecimalType(decimal.getPrecision(), decimal.getScale());
      }
      String name = logicalType.getName();
      if ("date".equals(name)) {
        return new VariantSchema.DateType();
      }
      if ("timestamp-micros".equals(name)) {
        return new VariantSchema.TimestampType();
      }
      if ("local-timestamp-micros".equals(name)) {
        return new VariantSchema.TimestampNTZType();
      }
    }

    switch (schema.getType()) {
      case BOOLEAN:
        return new VariantSchema.BooleanType();
      case INT:
        return new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT);
      case LONG:
        return new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG);
      case FLOAT:
        return new VariantSchema.FloatType();
      case DOUBLE:
        return new VariantSchema.DoubleType();
      case STRING:
        return new VariantSchema.StringType();
      case BYTES:
        return new VariantSchema.BinaryType();
      default:
        throw new IllegalStateException("Unexpected shreddable typed_value type: " + schema);
    }
  }

  /**
   * How the Avro value parquet-avro reads for a scalar {@code typed_value} leaf turns into the value
   * Spark's {@link ShreddingUtils#rebuild} expects. The file schema's Avro conversion drops these
   * encodings, so they are taken from the leaf's parquet type, as ParquetRowConverter does.
   */
  enum LeafDecoding {
    /** Boolean, Integer, Long, Float, Double, CharSequence, or bytes (ByteBuffer / GenericFixed), as is. */
    DIRECT,
    /** INT(32, false): the Integer is unsigned, widened to long. */
    UNSIGNED_INT32,
    /** INT(64, false): the Long is unsigned, read as DECIMAL(20, 0). */
    UNSIGNED_INT64,
    /** TIMESTAMP(MILLIS): the Long is in milliseconds, scaled to micros. */
    TIMESTAMP_MILLIS,
    /** DECIMAL on INT32 / INT64: the Integer / Long is the unscaled value. */
    DECIMAL_UNSCALED,
    /** DECIMAL on BINARY / FIXED_LEN_BYTE_ARRAY: big-endian two's-complement unscaled bytes. */
    DECIMAL_BYTES,
    /** BINARY (JSON): UTF-8 bytes, read as a string. */
    JSON
  }

  /** A scalar {@code typed_value} leaf: its Spark scalar type and how its Avro value is decoded. */
  private static final class ScalarLeaf {
    private final VariantSchema.ScalarType type;
    private final LeafDecoding decoding;

    private ScalarLeaf(VariantSchema.ScalarType type, LeafDecoding decoding) {
      this.type = type;
      this.decoding = decoding;
    }
  }

  /**
   * Builds the read-side {@link VariantSchema} of a shredded variant group from its parquet type, the
   * way Spark 4 does when it opens a file: SparkShreddingUtils.buildVariantSchema over the type that
   * ParquetToSparkSchemaConverter derives with ignoreVariantAnnotation = true. Ordinals are the parquet
   * field positions, as in Spark. Records each scalar leaf's {@link LeafDecoding} in {@code decodings},
   * and throws for layouts and leaf encodings Spark 4 rejects.
   */
  private static VariantSchema buildReadVariantSchema(GroupType group, boolean topLevel, String columnPath,
                                                      String groupPath, Map<VariantSchema, LeafDecoding> decodings) {
    if (group.getFieldCount() == 0) {
      throw invalidShreddingLayout(columnPath, groupPath, group);
    }
    int typedIdx = -1;
    int variantIdx = -1;
    int topLevelMetadataIdx = -1;
    ScalarLeaf scalarLeaf = null;
    VariantSchema.ObjectField[] objectSchema = null;
    VariantSchema arraySchema = null;

    for (int i = 0; i < group.getFieldCount(); i++) {
      Type field = group.getType(i);
      String fieldPath = groupPath + "." + field.getName();
      switch (field.getName()) {
        case HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD:
          if (typedIdx != -1) {
            throw invalidShreddingLayout(columnPath, groupPath, group);
          }
          typedIdx = i;
          if (field.isPrimitive()) {
            scalarLeaf = toScalarLeaf(field.asPrimitiveType(), columnPath, fieldPath);
          } else if (field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            Type element = listElementType(field.asGroupType());
            if (!isStructGroup(element)) {
              throw invalidShreddingLayout(columnPath, fieldPath, field);
            }
            arraySchema = buildReadVariantSchema(
                element.asGroupType(), false, columnPath, fieldPath + "." + element.getName(), decodings);
          } else if (isStructGroup(field)) {
            objectSchema = buildReadObjectSchema(field.asGroupType(), columnPath, fieldPath, decodings);
          } else {
            throw invalidShreddingLayout(columnPath, fieldPath, field);
          }
          break;
        case HoodieSchema.Variant.VARIANT_VALUE_FIELD:
          if (variantIdx != -1 || !isSparkBinary(field)) {
            throw invalidShreddingLayout(columnPath, groupPath, group);
          }
          variantIdx = i;
          break;
        case HoodieSchema.Variant.VARIANT_METADATA_FIELD:
          if (topLevelMetadataIdx != -1 || !isSparkBinary(field)) {
            throw invalidShreddingLayout(columnPath, groupPath, group);
          }
          topLevelMetadataIdx = i;
          break;
        default:
          throw invalidShreddingLayout(columnPath, groupPath, group);
      }
    }
    if (topLevel != (topLevelMetadataIdx >= 0)) {
      throw invalidShreddingLayout(columnPath, groupPath, group);
    }

    VariantSchema result = new VariantSchema(typedIdx, variantIdx, topLevelMetadataIdx, group.getFieldCount(),
        scalarLeaf == null ? null : scalarLeaf.type, objectSchema, arraySchema);
    if (scalarLeaf != null) {
      decodings.put(result, scalarLeaf.decoding);
    }
    return result;
  }

  /** An object {@code typed_value}: a non-empty group of uniquely named shredded field groups. */
  private static VariantSchema.ObjectField[] buildReadObjectSchema(GroupType typedValue, String columnPath,
                                                                   String typedValuePath,
                                                                   Map<VariantSchema, LeafDecoding> decodings) {
    if (typedValue.getFieldCount() == 0) {
      throw invalidShreddingLayout(columnPath, typedValuePath, typedValue);
    }
    VariantSchema.ObjectField[] fields = new VariantSchema.ObjectField[typedValue.getFieldCount()];
    Set<String> names = new HashSet<>();
    for (int i = 0; i < fields.length; i++) {
      Type field = typedValue.getType(i);
      if (!names.add(field.getName()) || !isStructGroup(field)) {
        throw invalidShreddingLayout(columnPath, typedValuePath, typedValue);
      }
      fields[i] = new VariantSchema.ObjectField(field.getName(), buildReadVariantSchema(
          field.asGroupType(), false, columnPath, typedValuePath + "." + field.getName(), decodings));
    }
    return fields;
  }

  /**
   * Maps a scalar {@code typed_value} leaf's parquet type to the Spark scalar type, mirroring
   * ParquetToSparkSchemaConverter.convertPrimitiveField at its defaults (binaryAsString = false,
   * inferTimestampNTZ = true, nanosAsLong = false) followed by SparkShreddingUtils.buildVariantSchema,
   * and to the matching {@link LeafDecoding}. INT96 is rejected although Spark reads it as a timestamp:
   * Hudi's Avro reader does not support it.
   */
  private static ScalarLeaf toScalarLeaf(PrimitiveType leaf, String columnPath, String leafPath) {
    // Leaf annotations are matched against a whitelist, so the UNKNOWN annotation (Spark's NullType,
    // not a shreddable scalar) is rejected without naming its class, which parquet < 1.16 lacks.
    LogicalTypeAnnotation annotation = leaf.getLogicalTypeAnnotation();
    switch (leaf.getPrimitiveTypeName()) {
      case BOOLEAN:
        if (annotation != null) {
          throw unsupportedLeaf(columnPath, leafPath, leaf);
        }
        return new ScalarLeaf(new VariantSchema.BooleanType(), LeafDecoding.DIRECT);
      case FLOAT:
        if (annotation != null) {
          throw unsupportedLeaf(columnPath, leafPath, leaf);
        }
        return new ScalarLeaf(new VariantSchema.FloatType(), LeafDecoding.DIRECT);
      case DOUBLE:
        if (annotation != null) {
          throw unsupportedLeaf(columnPath, leafPath, leaf);
        }
        return new ScalarLeaf(new VariantSchema.DoubleType(), LeafDecoding.DIRECT);
      case INT32:
        return toInt32Leaf(leaf, annotation, columnPath, leafPath);
      case INT64:
        return toInt64Leaf(leaf, annotation, columnPath, leafPath);
      case BINARY:
        if (annotation == null || annotation instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
          return new ScalarLeaf(new VariantSchema.BinaryType(), LeafDecoding.DIRECT);
        }
        if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
            || annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
          return new ScalarLeaf(new VariantSchema.StringType(), LeafDecoding.DIRECT);
        }
        if (annotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
          return new ScalarLeaf(new VariantSchema.StringType(), LeafDecoding.JSON);
        }
        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          return toDecimalLeaf(leaf, MAX_DECIMAL_PRECISION, LeafDecoding.DECIMAL_BYTES, columnPath, leafPath);
        }
        throw unsupportedLeaf(columnPath, leafPath, leaf);
      case FIXED_LEN_BYTE_ARRAY:
        if (annotation == null) {
          return new ScalarLeaf(new VariantSchema.BinaryType(), LeafDecoding.DIRECT);
        }
        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          return toDecimalLeaf(leaf, maxPrecisionForBytes(leaf.getTypeLength()), LeafDecoding.DECIMAL_BYTES,
              columnPath, leafPath);
        }
        // UUID, FLOAT16, INTERVAL: Spark rejects them.
        throw unsupportedLeaf(columnPath, leafPath, leaf);
      case INT96:
        throw new HoodieException("Cannot read shredded variant column '" + columnPath + "': typed_value '"
            + leafPath + "' has parquet type '" + leaf + "'. INT96 typed_value is not supported by Hudi's Avro reader.");
      default:
        throw unsupportedLeaf(columnPath, leafPath, leaf);
    }
  }

  private static ScalarLeaf toInt32Leaf(PrimitiveType leaf, LogicalTypeAnnotation annotation,
                                        String columnPath, String leafPath) {
    if (annotation == null) {
      return new ScalarLeaf(new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT), LeafDecoding.DIRECT);
    }
    if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      LogicalTypeAnnotation.IntLogicalTypeAnnotation intType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
      // Unsigned ints widen to the next signed size, as in Spark.
      switch (intType.getBitWidth()) {
        case 8:
          return new ScalarLeaf(new VariantSchema.IntegralType(intType.isSigned()
              ? VariantSchema.IntegralSize.BYTE : VariantSchema.IntegralSize.SHORT), LeafDecoding.DIRECT);
        case 16:
          return new ScalarLeaf(new VariantSchema.IntegralType(intType.isSigned()
              ? VariantSchema.IntegralSize.SHORT : VariantSchema.IntegralSize.INT), LeafDecoding.DIRECT);
        case 32:
          return intType.isSigned()
              ? new ScalarLeaf(new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT), LeafDecoding.DIRECT)
              : new ScalarLeaf(new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG), LeafDecoding.UNSIGNED_INT32);
        default:
          throw unsupportedLeaf(columnPath, leafPath, leaf);
      }
    }
    if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return new ScalarLeaf(new VariantSchema.DateType(), LeafDecoding.DIRECT);
    }
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return toDecimalLeaf(leaf, MAX_INT_DIGITS, LeafDecoding.DECIMAL_UNSCALED, columnPath, leafPath);
    }
    throw unsupportedLeaf(columnPath, leafPath, leaf);
  }

  private static ScalarLeaf toInt64Leaf(PrimitiveType leaf, LogicalTypeAnnotation annotation,
                                        String columnPath, String leafPath) {
    if (annotation == null) {
      return new ScalarLeaf(new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG), LeafDecoding.DIRECT);
    }
    if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      LogicalTypeAnnotation.IntLogicalTypeAnnotation intType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
      if (intType.getBitWidth() != 64) {
        throw unsupportedLeaf(columnPath, leafPath, leaf);
      }
      // DECIMAL(20, 0) holds the largest unsigned long, as in Spark.
      return intType.isSigned()
          ? new ScalarLeaf(new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG), LeafDecoding.DIRECT)
          : new ScalarLeaf(new VariantSchema.DecimalType(20, 0), LeafDecoding.UNSIGNED_INT64);
    }
    if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return toDecimalLeaf(leaf, MAX_LONG_DIGITS, LeafDecoding.DECIMAL_UNSCALED, columnPath, leafPath);
    }
    if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
      LeafDecoding decoding;
      switch (timestamp.getUnit()) {
        case MICROS:
          decoding = LeafDecoding.DIRECT;
          break;
        case MILLIS:
          decoding = LeafDecoding.TIMESTAMP_MILLIS;
          break;
        default:
          // NANOS: Spark reads it only as a plain long under its legacy nanosAsLong flag.
          throw unsupportedLeaf(columnPath, leafPath, leaf);
      }
      return new ScalarLeaf(timestamp.isAdjustedToUTC()
          ? new VariantSchema.TimestampType() : new VariantSchema.TimestampNTZType(), decoding);
    }
    // TIME included: Spark reads TIME(MICROS, local) as TimeType, which is not a shreddable scalar.
    throw unsupportedLeaf(columnPath, leafPath, leaf);
  }

  /** A decimal leaf whose precision must fit its physical type, as ParquetToSparkSchemaConverter checks. */
  private static ScalarLeaf toDecimalLeaf(PrimitiveType leaf, int maxPrecision, LeafDecoding decoding,
                                          String columnPath, String leafPath) {
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) leaf.getLogicalTypeAnnotation();
    int precision = decimal.getPrecision();
    int scale = decimal.getScale();
    if (precision < 1 || precision > maxPrecision || precision > MAX_DECIMAL_PRECISION
        || scale < 0 || scale > precision) {
      throw unsupportedLeaf(columnPath, leafPath, leaf);
    }
    return new ScalarLeaf(new VariantSchema.DecimalType(precision, scale), decoding);
  }

  /** Spark's Decimal.maxPrecisionForBytes: the digits a signed {@code numBytes}-byte integer always holds. */
  private static int maxPrecisionForBytes(int numBytes) {
    return (int) Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
  }

  /**
   * The element type of a parquet LIST group under Spark's list rules
   * (ParquetToSparkSchemaConverter.isElementType), or null for a malformed list. Applies only to the
   * typed_value list inside a variant; HoodieVariantReconstruction's planner walks the lists around
   * a variant column under parquet-avro's rules instead, so the two are not meant to agree.
   */
  private static Type listElementType(GroupType list) {
    if (list.getFieldCount() != 1 || !list.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return null;
    }
    Type repeated = list.getType(0);
    boolean repeatedIsElement = repeated.isPrimitive()
        || repeated.asGroupType().getFieldCount() > 1
        || repeated.getName().equals("array")
        || repeated.getName().equals(list.getName() + "_tuple");
    return repeatedIsElement ? repeated : repeated.asGroupType().getType(0);
  }

  /** Whether Spark reads {@code type} as a struct: a group that is neither a LIST nor a MAP. */
  private static boolean isStructGroup(Type type) {
    if (type == null || type.isPrimitive()) {
      return false;
    }
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    return !(annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation);
  }

  /** Whether Spark reads {@code type} as BinaryType, which the value and metadata fields must be. */
  private static boolean isSparkBinary(Type type) {
    if (!type.isPrimitive()) {
      return false;
    }
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    switch (type.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        return annotation == null || annotation instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
      case FIXED_LEN_BYTE_ARRAY:
        return annotation == null;
      default:
        return false;
    }
  }

  private static HoodieException unsupportedLeaf(String columnPath, String leafPath, PrimitiveType leaf) {
    return new HoodieException("Cannot read shredded variant column '" + columnPath + "': typed_value '"
        + leafPath + "' has parquet type '" + leaf + "', which Spark 4 does not support reading as a shredded "
        + "variant either.");
  }

  private static HoodieException invalidShreddingLayout(String columnPath, String path, Type type) {
    return new HoodieException("Cannot read shredded variant column '" + columnPath + "': '" + path
        + "' is not a valid shredded variant layout (" + type + "); Spark 4 rejects it too.");
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    if (buffer.hasArray() && buffer.position() == 0
        && buffer.arrayOffset() == 0
        && buffer.remaining() == buffer.array().length) {
      return buffer.array();
    }
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  /**
   * {@link ShreddedResult} implementation that collects shredded variant components
   * and converts them into an Avro {@link GenericRecord}.
   */
  static class AvroShreddedResult implements ShreddedResult {
    private final VariantSchema variantSchema;
    private final Schema avroSchema;

    private byte[] metadata;
    private byte[] variantValue;
    private Object scalarValue;
    private AvroShreddedResult[] objectFields;
    private AvroShreddedResult[] arrayElements;

    AvroShreddedResult(VariantSchema variantSchema, Schema avroSchema) {
      this.variantSchema = variantSchema;
      this.avroSchema = avroSchema;
    }

    @Override
    public void addArray(ShreddedResult[] array) {
      this.arrayElements = new AvroShreddedResult[array.length];
      for (int i = 0; i < array.length; i++) {
        this.arrayElements[i] = (AvroShreddedResult) array[i];
      }
    }

    @Override
    public void addObject(ShreddedResult[] values) {
      this.objectFields = new AvroShreddedResult[values.length];
      for (int i = 0; i < values.length; i++) {
        this.objectFields[i] = (AvroShreddedResult) values[i];
      }
    }

    @Override
    public void addVariantValue(byte[] result) {
      this.variantValue = result;
    }

    @Override
    public void addScalar(Object result) {
      this.scalarValue = result;
    }

    @Override
    public void addMetadata(byte[] result) {
      this.metadata = result;
    }

    /**
     * Converts the collected shredded components into an Avro {@link GenericRecord}.
     */
    GenericRecord toGenericRecord() {
      GenericRecord record = new GenericData.Record(avroSchema);

      // Metadata (only present at top level)
      if (metadata != null) {
        record.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(metadata));
      }

      // Value (variant binary for non-shredded or residual data)
      Schema.Field valueField = avroSchema.getField(HoodieSchema.Variant.VARIANT_VALUE_FIELD);
      if (valueField != null) {
        if (variantValue != null) {
          record.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(variantValue));
        } else {
          record.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, null);
        }
      }

      // Typed value
      Schema.Field tvField = avroSchema.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD);
      if (tvField == null) {
        return record;
      }

      if (scalarValue != null) {
        record.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, convertScalarToAvro(scalarValue));
      } else if (objectFields != null) {
        Schema tvSchema = unwrapNullable(tvField.schema());
        GenericRecord tvRecord = new GenericData.Record(tvSchema);
        for (int i = 0; i < objectFields.length; i++) {
          String fieldName = variantSchema.objectSchema[i].fieldName;
          // Always create the sub-record even for missing fields (non-null struct with null fields)
          tvRecord.put(fieldName, objectFields[i].toGenericRecord());
        }
        record.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, tvRecord);
      } else if (arrayElements != null) {
        List<GenericRecord> list = new ArrayList<>(arrayElements.length);
        for (AvroShreddedResult element : arrayElements) {
          list.add(element.toGenericRecord());
        }
        record.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, list);
      } else {
        record.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, null);
      }

      return record;
    }

    /**
     * Converts a scalar value from Spark's variant representation to an Avro-compatible type.
     * avroTypeToScalarType only emits scalar types whose Spark values (Boolean, Integer, Long, Float,
     * Double, String, BigDecimal) Avro takes as is, except binary, which Avro wants as a ByteBuffer.
     */
    private static Object convertScalarToAvro(Object value) {
      return value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) : value;
    }
  }

  /**
   * {@link ShreddedResultBuilder} that creates {@link AvroShreddedResult} instances
   * with the corresponding Avro schema at each nesting level.
   */
  static class AvroShreddedResultBuilder implements ShreddedResultBuilder {
    private final Map<VariantSchema, Schema> schemaMap = new IdentityHashMap<>();

    void registerSchema(VariantSchema variantSchema, Schema avroSchema) {
      schemaMap.put(variantSchema, avroSchema);
    }

    @Override
    public ShreddedResult createEmpty(VariantSchema schema) {
      Schema avroSchema = schemaMap.get(schema);
      if (avroSchema == null) {
        throw new IllegalStateException(
            "No Avro schema registered for VariantSchema: " + schema);
      }
      return new AvroShreddedResult(schema, avroSchema);
    }

    @Override
    public boolean allowNumericScaleChanges() {
      return true;
    }
  }

  /**
   * Base {@link ShreddingUtils.ShreddedRow} with all accessors throwing; concrete rows override
   * only the accessors valid for their nesting context. This is the read-path mirror of the
   * write-path {@link AvroShreddedResult}: it reads Avro records to feed Spark's reconstruction
   * ({@link ShreddingUtils#rebuild}).
   */
  abstract static class BaseAvroShreddedRow implements ShreddingUtils.ShreddedRow {
    @Override
    public boolean isNullAt(int ordinal) {
      throw unsupported();
    }

    @Override
    public boolean getBoolean(int ordinal) {
      throw unsupported();
    }

    @Override
    public byte getByte(int ordinal) {
      throw unsupported();
    }

    @Override
    public short getShort(int ordinal) {
      throw unsupported();
    }

    @Override
    public int getInt(int ordinal) {
      throw unsupported();
    }

    @Override
    public long getLong(int ordinal) {
      throw unsupported();
    }

    @Override
    public float getFloat(int ordinal) {
      throw unsupported();
    }

    @Override
    public double getDouble(int ordinal) {
      throw unsupported();
    }

    @Override
    public BigDecimal getDecimal(int ordinal, int precision, int scale) {
      throw unsupported();
    }

    @Override
    public String getString(int ordinal) {
      throw unsupported();
    }

    @Override
    public byte[] getBinary(int ordinal) {
      throw unsupported();
    }

    @Override
    public UUID getUuid(int ordinal) {
      throw unsupported();
    }

    @Override
    public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
      throw unsupported();
    }

    @Override
    public ShreddingUtils.ShreddedRow getArray(int ordinal) {
      throw unsupported();
    }

    @Override
    public int numElements() {
      throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException("Accessor not valid for this shredded row context");
    }
  }

  /**
   * A shredded variant struct {@code {value, [metadata], typed_value}}. Maps the Spark
   * {@link VariantSchema} ordinals (variantIdx / topLevelMetadataIdx / typedIdx) back to the named
   * Avro fields, and reads {@code typed_value} for scalar/object/array reconstruction, decoding a
   * scalar per its {@link LeafDecoding}.
   */
  static final class AvroVariantRow extends BaseAvroShreddedRow {
    private final GenericRecord record;
    private final VariantSchema schema;
    private final Map<VariantSchema, LeafDecoding> decodings;

    AvroVariantRow(GenericRecord record, VariantSchema schema, Map<VariantSchema, LeafDecoding> decodings) {
      this.record = record;
      this.schema = schema;
      this.decodings = decodings;
    }

    private String fieldNameFor(int ordinal) {
      if (ordinal == schema.typedIdx) {
        return HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD;
      }
      if (ordinal == schema.variantIdx) {
        return HoodieSchema.Variant.VARIANT_VALUE_FIELD;
      }
      if (ordinal == schema.topLevelMetadataIdx) {
        return HoodieSchema.Variant.VARIANT_METADATA_FIELD;
      }
      throw new IllegalArgumentException("Unexpected shredded ordinal: " + ordinal);
    }

    @Override public boolean isNullAt(int ordinal) {
      return record.get(fieldNameFor(ordinal)) == null;
    }

    @Override public byte[] getBinary(int ordinal) {
      return bytesOf(record.get(fieldNameFor(ordinal)));
    }

    // The scalar/struct/array getters below ignore `ordinal` and read typed_value directly via
    // typedValue(): Spark's rebuild() only invokes them for the scalar typed_value (ordinal ==
    // typedIdx), so dispatching via fieldNameFor(ordinal) would be redundant. isNullAt/getBinary
    // stay on fieldNameFor because rebuild() also calls them for the value/metadata fields.
    private Object typedValue() {
      return record.get(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD);
    }

    private LeafDecoding decoding() {
      return decodings.get(schema);
    }

    @Override public boolean getBoolean(int ordinal) {
      return (Boolean) typedValue();
    }

    @Override public byte getByte(int ordinal) {
      return ((Number) typedValue()).byteValue();
    }

    @Override public short getShort(int ordinal) {
      return ((Number) typedValue()).shortValue();
    }

    @Override public int getInt(int ordinal) {
      return ((Number) typedValue()).intValue();
    }

    @Override public long getLong(int ordinal) {
      Object value = typedValue();
      switch (decoding()) {
        case UNSIGNED_INT32:
          return Integer.toUnsignedLong((Integer) value);
        case TIMESTAMP_MILLIS:
          // Spark's DateTimeUtils.millisToMicros.
          return Math.multiplyExact((Long) value, 1000L);
        default:
          return ((Number) value).longValue();
      }
    }

    @Override public float getFloat(int ordinal) {
      return ((Number) typedValue()).floatValue();
    }

    @Override public double getDouble(int ordinal) {
      return ((Number) typedValue()).doubleValue();
    }

    @Override public String getString(int ordinal) {
      Object value = typedValue();
      return decoding() == LeafDecoding.JSON
          ? new String(bytesOf(value), StandardCharsets.UTF_8)
          : value.toString();
    }

    @Override public BigDecimal getDecimal(int ordinal, int precision, int scale) {
      Object value = typedValue();
      switch (decoding()) {
        case UNSIGNED_INT64:
          return new BigDecimal(Long.toUnsignedString((Long) value));
        case DECIMAL_UNSCALED:
          return BigDecimal.valueOf(((Number) value).longValue(), scale);
        case DECIMAL_BYTES:
          return value instanceof BigDecimal
              ? (BigDecimal) value
              : new BigDecimal(new BigInteger(bytesOf(value)), scale);
        default:
          throw new IllegalStateException("Unexpected decimal decoding: " + decoding());
      }
    }

    @Override public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
      // Object shredding: typed_value is a record whose fields are the shredded object fields.
      return new AvroObjectRow((GenericRecord) typedValue(), schema, decodings);
    }

    @Override public ShreddingUtils.ShreddedRow getArray(int ordinal) {
      return new AvroArrayRow((List<?>) typedValue(), schema.arraySchema, decodings);
    }
  }

  /**
   * The bytes of a binary Avro value: a ByteBuffer for parquet BINARY, a GenericFixed for
   * FIXED_LEN_BYTE_ARRAY.
   */
  private static byte[] bytesOf(Object value) {
    if (value instanceof ByteBuffer) {
      return toByteArray((ByteBuffer) value);
    }
    if (value instanceof GenericFixed) {
      return ((GenericFixed) value).bytes();
    }
    throw new IllegalStateException("Unexpected binary representation: " + value);
  }

  /**
   * The {@code typed_value} record of an object-shredded variant: ordinal {@code i} addresses the
   * i-th shredded object field (a nested {@code {value, typed_value}} struct).
   */
  static final class AvroObjectRow extends BaseAvroShreddedRow {
    private final GenericRecord typedValueRecord;
    private final VariantSchema parentSchema;
    private final Map<VariantSchema, LeafDecoding> decodings;

    AvroObjectRow(GenericRecord typedValueRecord, VariantSchema parentSchema,
                  Map<VariantSchema, LeafDecoding> decodings) {
      this.typedValueRecord = typedValueRecord;
      this.parentSchema = parentSchema;
      this.decodings = decodings;
    }

    @Override public boolean isNullAt(int ordinal) {
      return typedValueRecord.get(parentSchema.objectSchema[ordinal].fieldName) == null;
    }

    @Override public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
      VariantSchema.ObjectField field = parentSchema.objectSchema[ordinal];
      return new AvroVariantRow((GenericRecord) typedValueRecord.get(field.fieldName), field.schema, decodings);
    }
  }

  /**
   * The {@code typed_value} array of an array-shredded variant: each element is a shredded variant
   * struct following {@code elementSchema}.
   */
  static final class AvroArrayRow extends BaseAvroShreddedRow {
    private final List<?> elements;
    private final VariantSchema elementSchema;
    private final Map<VariantSchema, LeafDecoding> decodings;

    AvroArrayRow(List<?> elements, VariantSchema elementSchema, Map<VariantSchema, LeafDecoding> decodings) {
      this.elements = elements;
      this.elementSchema = elementSchema;
      this.decodings = decodings;
    }

    @Override public int numElements() {
      return elements.size();
    }

    @Override public boolean isNullAt(int ordinal) {
      return elements.get(ordinal) == null;
    }

    @Override public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
      return new AvroVariantRow((GenericRecord) elements.get(ordinal), elementSchema, decodings);
    }
  }
}
