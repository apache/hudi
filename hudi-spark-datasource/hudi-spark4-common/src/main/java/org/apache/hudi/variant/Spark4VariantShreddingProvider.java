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

import org.apache.hudi.avro.VariantShreddingProvider;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantSchema;
import org.apache.spark.types.variant.VariantShreddingWriter;
import org.apache.spark.types.variant.VariantShreddingWriter.ShreddedResult;
import org.apache.spark.types.variant.VariantShreddingWriter.ShreddedResultBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

  private static final String VALUE_FIELD = "value";
  private static final String METADATA_FIELD = "metadata";
  private static final String TYPED_VALUE_FIELD = "typed_value";

  @Override
  public GenericRecord shredVariantRecord(
      GenericRecord unshreddedVariant,
      Schema shreddedSchema,
      HoodieSchema.Variant variantSchema) {

    ByteBuffer valueBuf = (ByteBuffer) unshreddedVariant.get(VALUE_FIELD);
    ByteBuffer metadataBuf = (ByteBuffer) unshreddedVariant.get(METADATA_FIELD);

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

  /**
   * Builds a {@link VariantSchema} from an Avro {@link Schema} representing a
   * shredded variant structure ({@code value}, {@code metadata}, {@code typed_value}).
   *
   * <p>This method also registers the Avro schema mapping in the builder so that
   * {@link AvroShreddedResultBuilder#createEmpty} can create results with the
   * correct Avro schema at each nesting level.</p>
   */
  private VariantSchema buildVariantSchema(Schema avroSchema, boolean isTopLevel,
                                           AvroShreddedResultBuilder builder) {
    Schema.Field valueField = avroSchema.getField(VALUE_FIELD);
    Schema.Field metadataField = avroSchema.getField(METADATA_FIELD);
    Schema.Field typedValueField = avroSchema.getField(TYPED_VALUE_FIELD);

    int idx = 0;
    int variantIdx = valueField != null ? idx++ : -1;
    int topLevelMetadataIdx;
    if (metadataField != null && isTopLevel) {
      topLevelMetadataIdx = idx++;
    } else {
      topLevelMetadataIdx = -1;
      if (metadataField != null) {
        idx++;
      }
    }
    int typedIdx = typedValueField != null ? idx++ : -1;
    int numFields = idx;

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
          // Scalar shredding
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
   * Maps an Avro {@link Schema} type (potentially with logical type annotations)
   * to a {@link VariantSchema.ScalarType}.
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
      if ("timestamp-millis".equals(name)) {
        return new VariantSchema.TimestampType();
      }
      if ("local-timestamp-millis".equals(name)) {
        return new VariantSchema.TimestampNTZType();
      }
      if ("uuid".equals(name)) {
        return new VariantSchema.UuidType();
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
      case FIXED:
        return new VariantSchema.BinaryType();
      default:
        return null;
    }
  }

  private static Schema unwrapNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema type : schema.getTypes()) {
        if (type.getType() != Schema.Type.NULL) {
          return type;
        }
      }
    }
    return schema;
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
        record.put(METADATA_FIELD, ByteBuffer.wrap(metadata));
      }

      // Value (variant binary for non-shredded or residual data)
      Schema.Field valueField = avroSchema.getField(VALUE_FIELD);
      if (valueField != null) {
        if (variantValue != null) {
          record.put(VALUE_FIELD, ByteBuffer.wrap(variantValue));
        } else {
          record.put(VALUE_FIELD, null);
        }
      }

      // Typed value
      Schema.Field tvField = avroSchema.getField(TYPED_VALUE_FIELD);
      if (tvField == null) {
        return record;
      }

      if (scalarValue != null) {
        Schema tvSchema = unwrapNullable(tvField.schema());
        record.put(TYPED_VALUE_FIELD, convertScalarToAvro(scalarValue, tvSchema));
      } else if (objectFields != null) {
        Schema tvSchema = unwrapNullable(tvField.schema());
        GenericRecord tvRecord = new GenericData.Record(tvSchema);
        for (int i = 0; i < objectFields.length; i++) {
          String fieldName = variantSchema.objectSchema[i].fieldName;
          // Always create the sub-record even for missing fields (non-null struct with null fields)
          tvRecord.put(fieldName, objectFields[i].toGenericRecord());
        }
        record.put(TYPED_VALUE_FIELD, tvRecord);
      } else if (arrayElements != null) {
        List<GenericRecord> list = new ArrayList<>(arrayElements.length);
        for (AvroShreddedResult element : arrayElements) {
          list.add(element.toGenericRecord());
        }
        record.put(TYPED_VALUE_FIELD, list);
      } else {
        record.put(TYPED_VALUE_FIELD, null);
      }

      return record;
    }

    /**
     * Converts a scalar value from Spark's variant representation to an Avro-compatible type.
     * Handles type widening (Byte/Short to Int/Long) and binary wrapping.
     */
    private static Object convertScalarToAvro(Object value, Schema avroSchema) {
      if (value instanceof byte[]) {
        return ByteBuffer.wrap((byte[]) value);
      }
      if (value instanceof UUID) {
        return value.toString();
      }
      // Widen integer types to match Avro schema expectations
      if (avroSchema.getType() == Schema.Type.INT) {
        if (value instanceof Byte) {
          return ((Byte) value).intValue();
        }
        if (value instanceof Short) {
          return ((Short) value).intValue();
        }
      }
      if (avroSchema.getType() == Schema.Type.LONG) {
        if (value instanceof Byte) {
          return ((Byte) value).longValue();
        }
        if (value instanceof Short) {
          return ((Short) value).longValue();
        }
        if (value instanceof Integer) {
          return ((Integer) value).longValue();
        }
      }
      // BigDecimal, Boolean, String, Integer, Long, Float, Double
      // are directly compatible with Avro's type system
      return value;
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
}