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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchema.TimePrecision;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.VersionUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import scala.Enumeration;
import scala.Function1;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED;
import static org.apache.hudi.config.HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD;
import static org.apache.hudi.config.HoodieWriteConfig.AVRO_SCHEMA_STRING;
import static org.apache.hudi.config.HoodieWriteConfig.INTERNAL_SCHEMA_STRING;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Hoodie Write Support for directly writing Row to Parquet and adding the Hudi bloom index to the file metadata.
 * The implementation is adapted from Spark's {@link org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport} but has the following major differences:
 * <ul>
 *   <li>Decimals are always written with the legacy format to ensure compatibility with parquet-avro and other readers</li>
 *   <li>Writing 2-level or 3-level lists is configurable with either the Hudi option, hoodie.parquet.writelegacyformat.enabled, or the parquet-avro option, parquet.avro.write-old-list-structure
 *   to ensure consistency across writer paths.</li>
 *   <li>The scale of the timestamps is determined by the Hudi writer schema instead of relying on Spark configuration</li>
 * </ul>
 */
public class HoodieRowParquetWriteSupport extends WriteSupport<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRowParquetWriteSupport.class);
  private static final HoodieSchema MAP_KEY_SCHEMA = HoodieSchema.create(HoodieSchemaType.STRING);
  private static final String MAP_REPEATED_NAME = "key_value";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";
  private static final String SPARK_VARIANT_WRITE_SHREDDING_ENABLED = "spark.sql.variant.writeShredding.enabled";
  private static final String SPARK_VARIANT_ALLOW_READING_SHREDDED = "spark.sql.variant.allowReadingShredded";

  @Getter
  private final Configuration hadoopConf;
  private final Option<HoodieBloomFilterWriteSupport<UTF8String>> bloomFilterWriteSupportOpt;
  private final byte[] decimalBuffer = new byte[Decimal.minBytesForPrecision()[DecimalType.MAX_PRECISION()]];
  private final Enumeration.Value datetimeRebaseMode = (Enumeration.Value) SparkAdapterSupport$.MODULE$.sparkAdapter().getDateTimeRebaseMode();
  private final Function1<Object, Object> dateRebaseFunction = DataSourceUtils.createDateRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private final Function1<Object, Object> timestampRebaseFunction = DataSourceUtils.createTimestampRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private final boolean writeLegacyListFormat;
  private final ValueWriter[] rootFieldWriters;
  private final HoodieSchema schema;
  private final StructType structType;
  /**
   * The shredded schema. When Variant columns are configured for shredding, this schema has those VariantType columns replaced with their shredded struct schemas.
   * <p>
   * For non-shredded cases, this is identical to structType.
   */
  private final StructType shreddedSchema;
  private final boolean variantWriteShreddingEnabled;
  private final String variantForceShreddingSchemaForTest;
  private RecordConsumer recordConsumer;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    Configuration hadoopConf = new Configuration(conf);
    String writeLegacyFormatEnabled = config.getStringOrDefault(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED, "false");
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", writeLegacyFormatEnabled);
    hadoopConf.set("spark.sql.parquet.outputTimestampType", config.getStringOrDefault(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE));
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", config.getStringOrDefault(PARQUET_FIELD_ID_WRITE_ENABLED));

    // Variant shredding configs
    this.variantWriteShreddingEnabled = config.getBooleanOrDefault(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED);
    this.variantForceShreddingSchemaForTest = config.getString(PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST);
    hadoopConf.setBoolean(SPARK_VARIANT_WRITE_SHREDDING_ENABLED, variantWriteShreddingEnabled);
    hadoopConf.setBoolean(SPARK_VARIANT_ALLOW_READING_SHREDDED, config.getBooleanOrDefault(PARQUET_VARIANT_ALLOW_READING_SHREDDED));
    if (variantForceShreddingSchemaForTest != null && !variantForceShreddingSchemaForTest.isEmpty()) {
      hadoopConf.set("spark.sql.variant.forceShreddingSchemaForTest", variantForceShreddingSchemaForTest);
    }

    this.writeLegacyListFormat = Boolean.parseBoolean(writeLegacyFormatEnabled)
        || Boolean.parseBoolean(config.getStringOrDefault(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false"));
    this.structType = structType;
    // The avro schema is used to determine the precision for timestamps
    this.schema = SerDeHelper.fromJson(config.getString(INTERNAL_SCHEMA_STRING)).map(internalSchema -> InternalSchemaConverter.convert(internalSchema, "spark_schema"))
        .orElseGet(() -> {
          String schemaString = Option.ofNullable(config.getString(WRITE_SCHEMA_OVERRIDE)).orElseGet(() -> config.getString(AVRO_SCHEMA_STRING));
          HoodieSchema parsedSchema = HoodieSchema.parse(schemaString);
          return HoodieSchemaUtils.addMetadataFields(parsedSchema, config.getBooleanOrDefault(ALLOW_OPERATION_METADATA_FIELD));
        });
    // Generate shredded schema if there are shredded Variant columns.
    // Falls back to the provided schema if no shredded Variant columns are present.
    this.shreddedSchema = generateShreddedSchema(structType, schema);
    ParquetWriteSupport.setSchema(structType, hadoopConf);
    // Use shreddedSchema for creating writers when shredded Variants are present
    this.rootFieldWriters = getFieldWriters(shreddedSchema, schema);
    this.hadoopConf = hadoopConf;
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
  }

  /**
   * Generates a shredded schema from the given structType and hoodieSchema.
   * <p>
   * For Variant fields that are configured for shredding (based on HoodieSchema.Variant.isShredded()), the VariantType is replaced with a shredded struct schema.
   * <p>
   * Shredding behavior is controlled by:
   * <ul>
   *   <li>{@code hoodie.parquet.variant.write.shredding.enabled} - Master switch for shredding (default: true).
   *       When false, no shredding happens regardless of schema configuration.</li>
   *   <li>{@code hoodie.parquet.variant.force.shredding.schema.for.test} - When set, forces this DDL schema
   *       as the typed_value schema for ALL variant columns, overriding schema-driven shredding.</li>
   * </ul>
   *
   * This method recursively processes nested struct fields to handle Variant fields at any depth.
   *
   * @param structType The original Spark StructType
   * @param hoodieSchema The HoodieSchema containing shredding information
   * @return A StructType with shredded Variant fields replaced by their shredded schemas
   */
  private StructType generateShreddedSchema(StructType structType, HoodieSchema hoodieSchema) {
    // If write shredding is disabled, skip shredding entirely
    if (!variantWriteShreddingEnabled) {
      return structType;
    }

    // Parse forced shredding schema if configured
    StructType forcedShreddingSchema = null;
    if (variantForceShreddingSchemaForTest != null && !variantForceShreddingSchemaForTest.isEmpty()) {
      forcedShreddingSchema = StructType.fromDDL(variantForceShreddingSchemaForTest);
    }

    StructField[] fields = structType.fields();
    StructField[] shreddedFields = new StructField[fields.length];
    boolean hasShredding = false;

    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      DataType dataType = field.dataType();

      // If a forced shredding schema is configured, use it for all variant columns
      if (forcedShreddingSchema != null
          && SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantType(dataType)) {
        StructType markedShreddedStruct = SparkAdapterSupport$.MODULE$.sparkAdapter()
            .generateVariantWriteShreddingSchema(forcedShreddingSchema, true, false);
        shreddedFields[i] = new StructField(field.name(), markedShreddedStruct, field.nullable(), field.metadata());
        hasShredding = true;
        continue;
      }

      // Get the HoodieSchema for this field (if available)
      // Use getNonNullType() to unwrap nullable unions (e.g., ["null", "string"] -> "string")
      HoodieSchema fieldHoodieSchema = Option.ofNullable(hoodieSchema)
          .map(HoodieSchema::getNonNullType)
          .flatMap(s -> s.hasFields() ? s.getField(field.name()) : Option.empty())
          .map(HoodieSchemaField::schema)
          .orElse(null);

      if (SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantType(dataType)) {
        if (fieldHoodieSchema != null && fieldHoodieSchema.getType() == HoodieSchemaType.VARIANT) {
          HoodieSchema.Variant variantSchema = (HoodieSchema.Variant) fieldHoodieSchema;
          // If typed_value field exists, the variant is shredded
          if (variantSchema.getTypedValueField().isPresent()) {
            // Use plain types for SparkShreddingUtils (unwraps nested {value, typed_value} structs if present)
            HoodieSchema typedValueSchema = variantSchema.getPlainTypedValueSchema().get();
            DataType typedValueDataType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(typedValueSchema);

            // Generate the shredding schema with write metadata using SparkAdapter
            StructType markedShreddedStruct = SparkAdapterSupport$.MODULE$.sparkAdapter()
                .generateVariantWriteShreddingSchema(typedValueDataType, true, false);

            shreddedFields[i] = new StructField(field.name(), markedShreddedStruct, field.nullable(), field.metadata());
            hasShredding = true;
            continue;
          }
        } else {
          LOG.warn("Variant field '{}' has no corresponding HoodieSchema; "
              + "shredding will be skipped. This may indicate a schema mismatch.", field.name());
        }
      }

      // Recursively process nested fields (for VariantType without shredding, or for nested structures like StructType/ArrayType/MapType)
      DataType processedDataType = processNestedDataType(field.dataType(), fieldHoodieSchema);
      if (processedDataType != field.dataType()) {
        shreddedFields[i] = new StructField(field.name(), processedDataType, field.nullable(), field.metadata());
        hasShredding = true;
      } else {
        // No shredding in this field or its nested fields
        shreddedFields[i] = field;
      }
    }

    return hasShredding ? new StructType(shreddedFields) : structType;
  }

  /**
   * Recursively processes nested data types (structs, arrays, maps) to handle Variant shredding at any depth.
   *
   * @param dataType The data type to process
   * @param hoodieSchema The corresponding HoodieSchema
   * @return The processed data type with shredded Variants, or the original if no shredding needed
   */
  private DataType processNestedDataType(DataType dataType, HoodieSchema hoodieSchema) {
    // Unwrap nullable unions (e.g., ["null", "map"] -> "map") before type checks,
    // consistent with generateShreddedSchema and makeWriter
    HoodieSchema resolvedSchema = hoodieSchema != null ? hoodieSchema.getNonNullType() : null;

    // Check if this is a Variant type that should be shredded.
    // Both the Spark DataType and HoodieSchema must agree that this is a Variant,
    // consistent with generateShreddedSchema which also checks both.
    if (SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantType(dataType)
        && resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.VARIANT) {
      HoodieSchema.Variant variantSchema = (HoodieSchema.Variant) resolvedSchema;
      // If typed_value field exists, the variant is shredded
      if (variantSchema.getTypedValueField().isPresent()) {
        // Use plain types for SparkShreddingUtils (unwraps nested {value, typed_value} structs if present)
        HoodieSchema typedValueSchema = variantSchema.getPlainTypedValueSchema().get();
        DataType typedValueDataType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(typedValueSchema);

        // Generate the shredding schema with write metadata using SparkAdapter.
        // isTopLevel=true: this is the top level of the variant's own shredding structure.
        // isObjectField=false: this is a standalone variant column, not a field inside another variant's typed_value.
        return SparkAdapterSupport$.MODULE$.sparkAdapter()
            .generateVariantWriteShreddingSchema(typedValueDataType, true, false);
      }
      return dataType;
    }

    if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      return generateShreddedSchema(structType, resolvedSchema);
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      HoodieSchema elementSchema = resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.ARRAY
          ? resolvedSchema.getElementType()
          : null;
      DataType processedElementType = processNestedDataType(arrayType.elementType(), elementSchema);
      if (processedElementType != arrayType.elementType()) {
        return new ArrayType(processedElementType, arrayType.containsNull());
      }
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      HoodieSchema valueSchema = resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.MAP
          ? resolvedSchema.getValueType()
          : null;
      DataType processedValueType = processNestedDataType(mapType.valueType(), valueSchema);
      if (processedValueType != mapType.valueType()) {
        return new MapType(mapType.keyType(), processedValueType, mapType.valueContainsNull());
      }
    }
    return dataType;
  }

  /**
   * Creates field writers for each field in the schema.
   *
   * @param schema The schema to create writers for (may contain shredded Variant struct types)
   * @param hoodieSchema The HoodieSchema for type information
   * @return Array of ValueWriters for each field
   */
  private ValueWriter[] getFieldWriters(StructType schema, HoodieSchema hoodieSchema) {
    StructField[] fields = schema.fields();
    ValueWriter[] writers = new ValueWriter[fields.length];

    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];

      HoodieSchema fieldSchema = Option.ofNullable(hoodieSchema)
          .map(HoodieSchema::getNonNullType)
          .flatMap(s -> s.hasFields() ? s.getField(field.name()) : Option.empty())
          // Note: Cannot use HoodieSchemaField::schema method reference due to Java 17 compilation ambiguity
          .map(f -> f.schema())
          .orElse(null);

      writers[i] = makeWriter(fieldSchema, field.dataType());
    }

    return writers;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("org.apache.spark.version", VersionUtils.shortVersion(HoodieSparkUtils.getSparkVersion()));
    if (SparkAdapterSupport$.MODULE$.sparkAdapter().isLegacyBehaviorPolicy(datetimeRebaseMode)) {
      metadata.put("org.apache.spark.legacyDateTime", "");
      metadata.put("org.apache.spark.timeZone", SQLConf.get().sessionLocalTimeZone());
    }
    String vectorMeta = HoodieSchema.buildVectorColumnsMetadataValue(schema);
    if (!vectorMeta.isEmpty()) {
      metadata.put(HoodieSchema.PARQUET_VECTOR_COLUMNS_METADATA_KEY, vectorMeta);
    }
    Configuration configurationCopy = new Configuration(configuration);
    configurationCopy.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, Boolean.toString(writeLegacyListFormat));
    // Use shreddedSchema for Parquet schema conversion when shredding is enabled
    // This ensures the Parquet file structure includes the shredded typed_value columns
    MessageType messageType = convert(shreddedSchema, schema);
    return new WriteContext(messageType, metadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(InternalRow row) {
    consumeMessage(() -> writeFields(row, structType, rootFieldWriters));
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
            .orElse(Collections.emptyMap());

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(UTF8String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }

  @FunctionalInterface
  private interface ValueWriter {
    void write(SpecializedGetters row, int ordinal);
  }

  private void consumeMessage(Runnable writer) {
    recordConsumer.startMessage();
    writer.run();
    recordConsumer.endMessage();
  }

  private void consumeGroup(Runnable writer) {
    recordConsumer.startGroup();
    writer.run();
    recordConsumer.endGroup();
  }

  private void consumeField(String field, int index, Runnable writer) {
    recordConsumer.startField(field, index);
    writer.run();
    recordConsumer.endField(field, index);
  }

  private void writeFields(InternalRow row, StructType schema, ValueWriter[] fieldWriters) {
    for (int i = 0; i < row.numFields(); i++) {
      int index = i;
      if (!row.isNullAt(i)) {
        consumeField(schema.fields()[i].name(), index, () -> fieldWriters[index].write(row, index));
      }
    }
  }

  private ValueWriter makeWriter(HoodieSchema schema, DataType dataType) {
    HoodieSchema resolvedSchema = schema == null ? null : schema.getNonNullType();

    if (dataType == DataTypes.BooleanType) {
      return (row, ordinal) -> recordConsumer.addBoolean(row.getBoolean(ordinal));
    } else if (dataType == DataTypes.DateType) {
      return (row, ordinal) -> recordConsumer.addInteger((Integer) dateRebaseFunction.apply(row.getInt(ordinal)));
    } else if (dataType == DataTypes.ShortType) {
      return (row, ordinal) -> recordConsumer.addInteger(row.getShort(ordinal));
    } else if (dataType == DataTypes.IntegerType || dataType instanceof YearMonthIntervalType) {
      return (row, ordinal) -> recordConsumer.addInteger(row.getInt(ordinal));
    } else if (dataType == DataTypes.LongType || dataType instanceof DayTimeIntervalType) {
      return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
    } else if (dataType == DataTypes.TimestampType) {
      if (resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.TIMESTAMP) {
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) resolvedSchema;
        if (timestampSchema.getPrecision() == TimePrecision.MICROS) {
          return (row, ordinal) -> recordConsumer.addLong((long) timestampRebaseFunction.apply(row.getLong(ordinal)));
        } else if (timestampSchema.getPrecision() == TimePrecision.MILLIS) {
          return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis((long) timestampRebaseFunction.apply(row.getLong(ordinal))));
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision for TimestampType: " + timestampSchema.getPrecision());
        }
      } else {
        // Default to micros precision when no timestamp schema is available
        return (row, ordinal) -> recordConsumer.addLong((long) timestampRebaseFunction.apply(row.getLong(ordinal)));
      }
    } else if (SparkAdapterSupport$.MODULE$.sparkAdapter().isTimestampNTZType(dataType)) {
      if (resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.TIMESTAMP) {
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) resolvedSchema;
        if (!timestampSchema.isUtcAdjusted()) {
          if (timestampSchema.getPrecision() == TimePrecision.MICROS) {
            return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
          } else if (timestampSchema.getPrecision() == TimePrecision.MILLIS) {
            return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis(row.getLong(ordinal)));
          } else {
            throw new UnsupportedOperationException("Unsupported timestamp precision for TimestampNTZType: " + timestampSchema.getPrecision());
          }
        } else {
          throw new UnsupportedOperationException("TimestampNTZType requires local timestamp schema, but got UTC-adjusted: " + timestampSchema.getName());
        }
      } else {
        // Default to micros precision when no timestamp schema is available
        return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
      }
    } else if (dataType == DataTypes.FloatType) {
      return (row, ordinal) -> recordConsumer.addFloat(row.getFloat(ordinal));
    } else if (dataType == DataTypes.DoubleType) {
      return (row, ordinal) -> recordConsumer.addDouble(row.getDouble(ordinal));
    } else if (dataType == DataTypes.StringType) {
      return (row, ordinal) -> recordConsumer.addBinary(
          Binary.fromReusedByteArray(row.getUTF8String(ordinal).getBytes()));
    } else if (dataType == DataTypes.BinaryType) {
      return (row, ordinal) -> recordConsumer.addBinary(
          Binary.fromReusedByteArray(row.getBinary(ordinal)));
    } else if (SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantType(dataType)) {
      // Maps VariantType to a Parquet group with 'metadata' and 'value' binary fields.
      // Field order follows the Parquet spec (metadata at index 0, value at index 1).
      // Note: We intentionally omit 'typed_value' for shredded variants as this writer only accesses raw binary blobs.
      BiConsumer<SpecializedGetters, Integer> variantWriter = SparkAdapterSupport$.MODULE$.sparkAdapter().createVariantValueWriter(
          dataType,
          valueBytes -> consumeField(HoodieSchema.Variant.VARIANT_VALUE_FIELD, 1, () -> recordConsumer.addBinary(Binary.fromConstantByteArray(valueBytes))),
          metadataBytes -> consumeField(HoodieSchema.Variant.VARIANT_METADATA_FIELD, 0, () -> recordConsumer.addBinary(Binary.fromConstantByteArray(metadataBytes)))
      );
      return (row, ordinal) -> {
        consumeGroup(() -> variantWriter.accept(row, ordinal));
      };
    } else if (dataType instanceof DecimalType) {
      return (row, ordinal) -> {
        int precision = ((DecimalType) dataType).precision();
        ValidationUtils.checkArgument(precision <= DecimalType.MAX_PRECISION(),
            () -> String.format("Decimal precision %s exceeds max precision %s", precision, DecimalType.MAX_PRECISION()));
        int scale = ((DecimalType) dataType).scale();
        byte[] bytes = row.getDecimal(ordinal, precision, scale).toJavaBigDecimal().unscaledValue().toByteArray();
        int numBytes = Decimal.minBytesForPrecision()[precision];
        byte[] fixedLengthBytes;
        if (bytes.length == numBytes) {
          // If the length of the underlying byte array of the unscaled `BigInteger` happens to be
          // `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
          fixedLengthBytes = bytes;
        } else {
          // Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
          // the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
          // fixed-length byte array.
          byte signByte = (bytes[0] < 0) ? (byte) -1 : (byte) 0;
          Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
          System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
          fixedLengthBytes = decimalBuffer;
        }
        recordConsumer.addBinary(Binary.fromReusedByteArray(fixedLengthBytes, 0, numBytes));
      };
    } else if (dataType instanceof ArrayType
            && resolvedSchema != null
            && resolvedSchema.getType() == HoodieSchemaType.VECTOR) {
      HoodieSchema.Vector vectorSchema = (HoodieSchema.Vector) resolvedSchema;
      int dimension = vectorSchema.getDimension();
      HoodieSchema.Vector.VectorElementType elemType = vectorSchema.getVectorElementType();
      int bufferSize = Math.multiplyExact(dimension, elemType.getElementSize());
      // Buffer is reused across rows without copying. Binary.fromReusedByteArray wraps the
      // same backing array. This is safe because Parquet's ColumnWriteStoreV2 consumes the
      // bytes before the next write call (same pattern as the decimal path with decimalBuffer).
      ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
      return (row, ordinal) -> {
        ArrayData array = row.getArray(ordinal);
        ValidationUtils.checkArgument(array.numElements() == dimension,
            () -> String.format("Vector dimension mismatch: schema expects %d elements but got %d", dimension, array.numElements()));
        buffer.clear();
        switch (elemType) {
          case FLOAT:
            for (int i = 0; i < dimension; i++) {
              buffer.putFloat(array.getFloat(i));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < dimension; i++) {
              buffer.putDouble(array.getDouble(i));
            }
            break;
          case INT8:
            for (int i = 0; i < dimension; i++) {
              buffer.put(array.getByte(i));
            }
            break;
          default:
            throw new UnsupportedOperationException("Unsupported vector element type: " + elemType);
        }
        recordConsumer.addBinary(Binary.fromReusedByteArray(buffer.array()));
      };
    } else if (dataType instanceof ArrayType) {
      ValueWriter elementWriter = makeWriter(resolvedSchema == null ? null : resolvedSchema.getElementType(), ((ArrayType) dataType).elementType());
      if (!writeLegacyListFormat) {
        return threeLevelArrayWriter("list", "element", elementWriter);
      } else if (((ArrayType) dataType).containsNull()) {
        return threeLevelArrayWriter("bag", "array", elementWriter);
      } else {
        return twoLevelArrayWriter("array", elementWriter);
      }
    } else if (dataType instanceof MapType) {
      ValueWriter keyWriter = makeWriter(MAP_KEY_SCHEMA, DataTypes.StringType);
      ValueWriter valueWriter = makeWriter(resolvedSchema == null ? null : resolvedSchema.getValueType(), ((MapType) dataType).valueType());
      return (row, ordinal) -> {
        MapData mapData = row.getMap(ordinal);
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        consumeGroup(() -> {
          if (mapData.numElements() > 0) {
            consumeField(MAP_REPEATED_NAME, 0, () -> {
              for (int i = 0; i < mapData.numElements(); i++) {
                int index = i;
                consumeGroup(() -> {
                  consumeField(MAP_KEY_NAME, 0, () -> keyWriter.write(keyArray, index));
                  if (!valueArray.isNullAt(index)) {
                    consumeField(MAP_VALUE_NAME, 1, () -> valueWriter.write(valueArray, index));
                  }
                });
              }
            });
          }
        });
      };
    } else if (dataType instanceof StructType
        && SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantShreddingStruct((StructType) dataType)) {
      return makeShreddedVariantWriter((StructType) dataType);
    } else if (dataType instanceof StructType) {
      StructType structType = (StructType) dataType;
      ValueWriter[] fieldWriters = getFieldWriters(structType, resolvedSchema);
      return (row, ordinal) -> {
        InternalRow struct = row.getStruct(ordinal, fieldWriters.length);
        consumeGroup(() -> writeFields(struct, structType, fieldWriters));
      };
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  /**
   * Creates a ValueWriter for a shredded Variant column.
   * This writer converts a Variant value into its shredded components (metadata, value, typed_value) and writes them to Parquet.
   *
   * @param shreddedStructType The shredded StructType (with shredding metadata)
   * @return A ValueWriter that handles shredded Variant writing
   */
  private ValueWriter makeShreddedVariantWriter(StructType shreddedStructType) {
    // Create writers for the shredded struct fields
    // The shreddedStructType contains: metadata (binary), value (binary), typed_value (optional)
    ValueWriter[] shreddedFieldWriters = Arrays.stream(shreddedStructType.fields())
        .map(field -> makeWriter(null, field.dataType()))
        .toArray(ValueWriter[]::new);

    // Use the SparkAdapter to create a shredded variant writer that converts Variant to shredded components
    BiConsumer<SpecializedGetters, Integer> shreddedWriter = SparkAdapterSupport$.MODULE$.sparkAdapter()
        .createShreddedVariantWriter(
            shreddedStructType,
            shreddedRow -> {
              // Write the shredded row as a group
              consumeGroup(() -> writeFields(shreddedRow, shreddedStructType, shreddedFieldWriters));
            }
        );

    return shreddedWriter::accept;
  }

  private ValueWriter twoLevelArrayWriter(String repeatedFieldName, ValueWriter elementWriter) {
    return (row, ordinal) -> {
      ArrayData array = row.getArray(ordinal);
      consumeGroup(() -> {
        if (array.numElements() > 0) {
          consumeField(repeatedFieldName, 0, () -> {
            for (int i = 0; i < array.numElements(); i++) {
              elementWriter.write(row.getArray(ordinal), i);
            }
          });
        }
      });
    };
  }

  private ValueWriter threeLevelArrayWriter(String repeatedFieldName, String elementFieldName, ValueWriter elementWriter) {
    return (row, ordinal) -> {
      ArrayData array = row.getArray(ordinal);
      consumeGroup(() -> {
        if (array.numElements() > 0) {
          consumeField(repeatedFieldName, 0, () -> {
            for (int i = 0; i < array.numElements(); i++) {
              int index = i;
              consumeGroup(() -> {
                if (!array.isNullAt(index)) {
                  consumeField(elementFieldName, 0, () -> elementWriter.write(array, index));
                }
              });
            }
          });
        }
      });
    };
  }

  public static HoodieRowParquetWriteSupport getHoodieRowParquetWriteSupport(Configuration conf, StructType structType,
                                                                             Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    return (HoodieRowParquetWriteSupport) ReflectionUtils.loadClass(
        config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS),
        new Class<?>[] {Configuration.class, StructType.class, Option.class, HoodieConfig.class},
        conf, structType, bloomFilterOpt, config);
  }

  /**
   * Constructs the Parquet schema based on the given Spark schema and Avro schema.
   * The Avro schema is used to determine the precision of timestamps.
   * @param structType Spark StructType
   * @param schema Hoodie schema
   * @return Parquet MessageType with field ids propagated from Spark StructType if available
   */
  private MessageType convert(StructType structType, HoodieSchema schema) {
    return Types.buildMessage()
        .addFields(Arrays.stream(structType.fields()).map(field -> {
          // Note: Cannot use HoodieSchemaField::schema method reference due to Java 17 compilation ambiguity
          HoodieSchema fieldSchema = schema.getField(field.name())
              .map(f -> f.schema())
              .orElse(null);
          return convertField(fieldSchema, field);
        }).toArray(Type[]::new))
        .named("spark_schema");
  }

  private Type convertField(HoodieSchema fieldSchema, StructField structField) {
    Type type = convertField(fieldSchema, structField, structField.nullable() ? OPTIONAL : REQUIRED);
    if (ParquetUtils.hasFieldId(structField)) {
      return type.withId(ParquetUtils.getFieldId(structField));
    }
    return type;
  }

  private Type convertField(HoodieSchema fieldSchema, StructField structField, Type.Repetition repetition) {
    HoodieSchema resolvedSchema = fieldSchema == null ? null : fieldSchema.getNonNullType();

    DataType dataType = structField.dataType();
    if (dataType == DataTypes.BooleanType) {
      return Types.primitive(BOOLEAN, repetition).named(structField.name());
    } else if (dataType == DataTypes.DateType) {
      return Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.dateType()).named(structField.name());
    } else if (dataType == DataTypes.ShortType) {
      return Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(16, true)).named(structField.name());
    } else if (dataType == DataTypes.IntegerType || dataType instanceof YearMonthIntervalType) {
      return Types.primitive(INT32, repetition).named(structField.name());
    } else if (dataType == DataTypes.LongType || dataType instanceof DayTimeIntervalType) {
      return Types.primitive(INT64, repetition).named(structField.name());
    } else if (dataType == DataTypes.TimestampType) {
      if (resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.TIMESTAMP) {
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) resolvedSchema;
        if (timestampSchema.getPrecision() == TimePrecision.MICROS) {
          return Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
        } else if (timestampSchema.getPrecision() == TimePrecision.MILLIS) {
          return Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(structField.name());
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision for TimestampType: " + timestampSchema.getPrecision());
        }
      } else {
        // Default to micros precision when no timestamp schema is available
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
      }
    } else if (SparkAdapterSupport$.MODULE$.sparkAdapter().isTimestampNTZType(dataType)) {
      if (resolvedSchema != null && resolvedSchema.getType() == HoodieSchemaType.TIMESTAMP) {
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) resolvedSchema;
        if (!timestampSchema.isUtcAdjusted()) {
          if (timestampSchema.getPrecision() == TimePrecision.MICROS) {
            return Types.primitive(INT64, repetition)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
          } else if (timestampSchema.getPrecision() == TimePrecision.MILLIS) {
            return Types.primitive(INT64, repetition)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(structField.name());
          } else {
            throw new UnsupportedOperationException("Unsupported timestamp precision for TimestampNTZType: " + timestampSchema.getPrecision());
          }
        } else {
          throw new UnsupportedOperationException("TimestampNTZType requires local timestamp schema, but got UTC-adjusted: " + timestampSchema.getName());
        }
      } else {
        // Default to micros precision when no timestamp schema is available
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
      }
    } else if (dataType == DataTypes.FloatType) {
      return Types.primitive(FLOAT, repetition).named(structField.name());
    } else if (dataType == DataTypes.DoubleType) {
      return Types.primitive(DOUBLE, repetition).named(structField.name());
    } else if (dataType == DataTypes.StringType) {
      return Types.primitive(BINARY, repetition)
          .as(LogicalTypeAnnotation.stringType()).named(structField.name());
    } else if (dataType == DataTypes.BinaryType) {
      return Types.primitive(BINARY, repetition).named(structField.name());
    } else if (SparkAdapterSupport$.MODULE$.sparkAdapter().isVariantType(dataType)) {
      return SparkAdapterSupport$.MODULE$.sparkAdapter().convertVariantFieldToParquetType(
          dataType,
          structField.name(),
          resolvedSchema,
          repetition
      );
    } else if (dataType instanceof DecimalType) {
      int precision = ((DecimalType) dataType).precision();
      int scale = ((DecimalType) dataType).scale();
      return Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .length(Decimal.minBytesForPrecision()[precision])
          .named(structField.name());
    } else if (dataType instanceof ArrayType
            && resolvedSchema != null
            && resolvedSchema.getType() == HoodieSchemaType.VECTOR) {
      HoodieSchema.Vector vectorSchema = (HoodieSchema.Vector) resolvedSchema;
      int fixedSize = Math.multiplyExact(vectorSchema.getDimension(),
              vectorSchema.getVectorElementType().getElementSize());
      return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
              .length(fixedSize).named(structField.name());
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType elementType = arrayType.elementType();
      HoodieSchema elementSchema = resolvedSchema == null ? null : resolvedSchema.getElementType();
      if (!writeLegacyListFormat) {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            .addField(
                Types.repeatedGroup()
                    .addField(convertField(elementSchema, new StructField("element", elementType, arrayType.containsNull(), Metadata.empty())))
                    .named("list"))
            .named(structField.name());
      } else if ((arrayType.containsNull())) {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            .addField(Types
                .buildGroup(REPEATED)
                // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
                .addField(convertField(elementSchema, new StructField("array", elementType, true, Metadata.empty())))
                .named("bag"))
            .named(structField.name());
      } else {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
            .addField(convertField(elementSchema, new StructField("array", elementType, false, Metadata.empty()), REPEATED))
            .named(structField.name());
      }
    } else if (dataType instanceof MapType) {
      HoodieSchema valueSchema = resolvedSchema == null ? null : resolvedSchema.getValueType();
      MapType mapType = (MapType) dataType;
      return Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.mapType())
          .addField(
              Types
                  .repeatedGroup()
                  .addField(convertField(MAP_KEY_SCHEMA, new StructField(MAP_KEY_NAME, DataTypes.StringType, false, Metadata.empty())))
                  .addField(convertField(valueSchema, new StructField(MAP_VALUE_NAME, mapType.valueType(), mapType.valueContainsNull(), Metadata.empty())))
                  .named(MAP_REPEATED_NAME))
          .named(structField.name());
    } else if (dataType instanceof StructType) {
      Types.GroupBuilder<GroupType> groupBuilder = Types.buildGroup(repetition);
      Arrays.stream(((StructType) dataType).fields()).forEach(field -> {
        // Note: Cannot use HoodieSchemaField::schema method reference due to Java 17 compilation ambiguity
        HoodieSchema nestedFieldSchema = Option.ofNullable(resolvedSchema)
            .flatMap(s -> s.getField(field.name()))
            .map(f -> f.schema())
            .orElse(null);
        groupBuilder.addField(convertField(nestedFieldSchema, field));
      });
      return groupBuilder.named(structField.name());
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }
}
