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

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
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
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.VersionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import scala.Enumeration;
import scala.Function1;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED;
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

  private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final String MAP_REPEATED_NAME = "key_value";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";

  private final Configuration hadoopConf;
  private final Option<HoodieBloomFilterWriteSupport<UTF8String>> bloomFilterWriteSupportOpt;
  private final byte[] decimalBuffer = new byte[Decimal.minBytesForPrecision()[DecimalType.MAX_PRECISION()]];
  private final Enumeration.Value datetimeRebaseMode = (Enumeration.Value) SparkAdapterSupport$.MODULE$.sparkAdapter().getDateTimeRebaseMode();
  private final Function1<Object, Object> dateRebaseFunction = DataSourceUtils.createDateRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private final Function1<Object, Object> timestampRebaseFunction = DataSourceUtils.createTimestampRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private RecordConsumer recordConsumer;
  private final boolean writeLegacyListFormat;
  private final ValueWriter[] rootFieldWriters;
  private final Schema avroSchema;
  private final StructType structType;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType structType, Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    Configuration hadoopConf = new Configuration(conf);
    String writeLegacyFormatEnabled = config.getStringOrDefault(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED, "false");
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", writeLegacyFormatEnabled);
    hadoopConf.set("spark.sql.parquet.outputTimestampType", config.getStringOrDefault(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE));
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", config.getStringOrDefault(PARQUET_FIELD_ID_WRITE_ENABLED));
    this.writeLegacyListFormat = Boolean.parseBoolean(writeLegacyFormatEnabled)
        || Boolean.parseBoolean(config.getStringOrDefault(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false"));
    this.structType = structType;
    // The avro schema is used to determine the precision for timestamps
    this.avroSchema = SerDeHelper.fromJson(config.getString(INTERNAL_SCHEMA_STRING)).map(internalSchema -> AvroInternalSchemaConverter.convert(internalSchema, "spark_schema"))
        .orElseGet(() -> {
          String schemaString = Option.ofNullable(config.getString(WRITE_SCHEMA_OVERRIDE)).orElseGet(() -> config.getString(AVRO_SCHEMA_STRING));
          Schema parsedSchema = new Schema.Parser().parse(schemaString);
          return HoodieAvroUtils.addMetadataFields(parsedSchema, config.getBooleanOrDefault(ALLOW_OPERATION_METADATA_FIELD));
        });
    ParquetWriteSupport.setSchema(structType, hadoopConf);
    this.rootFieldWriters = getFieldWriters(structType, avroSchema);
    this.hadoopConf = hadoopConf;
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
  }

  private ValueWriter[] getFieldWriters(StructType schema, Schema avroSchema) {
    return Arrays.stream(schema.fields()).map(field -> {
      Schema.Field avroField = avroSchema.getField(field.name());
      return makeWriter(avroField == null ? null : avroField.schema(), field.dataType());
    }).toArray(ValueWriter[]::new);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("org.apache.spark.version", VersionUtils.shortVersion(HoodieSparkUtils.getSparkVersion()));
    if (SparkAdapterSupport$.MODULE$.sparkAdapter().isLegacyBehaviorPolicy(datetimeRebaseMode)) {
      metadata.put("org.apache.spark.legacyDateTime", "");
      metadata.put("org.apache.spark.timeZone", SQLConf.get().sessionLocalTimeZone());
    }
    Configuration configurationCopy = new Configuration(configuration);
    configurationCopy.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, Boolean.toString(writeLegacyListFormat));
    MessageType messageType = convert(structType, avroSchema);
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
    for (int i = 0; i < fieldWriters.length; i++) {
      int index = i;
      if (!row.isNullAt(i)) {
        StructField field = schema.fields()[i];
        consumeField(field.name(), index, () -> fieldWriters[index].write(row, index));
      }
    }
  }

  private ValueWriter makeWriter(Schema avroSchema, DataType dataType) {
    Schema resolvedSchema = avroSchema == null ? null : resolveNullableSchema(avroSchema);
    LogicalType logicalType = resolvedSchema != null ? resolvedSchema.getLogicalType() : null;

    if (dataType == DataTypes.BooleanType) {
      return (row, ordinal) -> recordConsumer.addBoolean(row.getBoolean(ordinal));
    } else if (dataType == DataTypes.DateType) {
      return (row, ordinal) -> recordConsumer.addInteger((Integer) dateRebaseFunction.apply(row.getInt(ordinal)));
    } else if (dataType == DataTypes.ShortType) {
      return (row, ordinal) -> recordConsumer.addInteger(row.getShort(ordinal));
    } else if (dataType == DataTypes.IntegerType) {
      return (row, ordinal) -> recordConsumer.addInteger(row.getInt(ordinal));
    } else if (dataType == DataTypes.LongType) {
      return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
    } else if (dataType == DataTypes.TimestampType) {
      if (logicalType == null || logicalType.getName().equals(LogicalTypes.timestampMicros().getName())) {
        return (row, ordinal) -> recordConsumer.addLong((long) timestampRebaseFunction.apply(row.getLong(ordinal)));
      } else if (logicalType.getName().equals(LogicalTypes.timestampMillis().getName())) {
        return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis((long) timestampRebaseFunction.apply(row.getLong(ordinal))));
      } else {
        throw new UnsupportedOperationException("Unsupported timestamp type: " + logicalType);
      }
    } else if (dataType == DataTypes.TimestampNTZType) {
      if (logicalType == null || logicalType.getName().equals(LogicalTypes.localTimestampMicros().getName())) {
        return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
      } else if (logicalType.getName().equals(LogicalTypes.localTimestampMillis().getName())) {
        return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis(row.getLong(ordinal)));
      } else {
        throw new UnsupportedOperationException("Unsupported timestamp type: " + logicalType);
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
    } else if (dataType instanceof DecimalType) {
      return (row, ordinal) -> {
        int precision = ((DecimalType) dataType).precision();
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

  private static class HoodieBloomFilterRowWriteSupport extends HoodieBloomFilterWriteSupport<UTF8String> {
    public HoodieBloomFilterRowWriteSupport(BloomFilter bloomFilter) {
      super(bloomFilter);
    }

    @Override
    protected byte[] getUTF8Bytes(UTF8String key) {
      return key.getBytes();
    }

    @Override
    protected UTF8String dereference(UTF8String key) {
      // NOTE: [[clone]] is performed here (rather than [[copy]]) to only copy underlying buffer in
      //       cases when [[UTF8String]] is pointing into a buffer storing the whole containing record,
      //       and simply do a pass over when it holds a (immutable) buffer holding just the string
      return key.clone();
    }
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
   * @param avroSchema Avro schema
   * @return Parquet MessageType with field ids propagated from Spark StructType if available
   */
  private MessageType convert(StructType structType, Schema avroSchema) {
    return Types.buildMessage()
        .addFields(Arrays.stream(structType.fields()).map(field -> {
          Schema.Field avroField = avroSchema.getField(field.name());
          Type.Repetition repetition = field.nullable() ? OPTIONAL : REQUIRED;
          Type type = convertField(avroField == null ? null : avroField.schema(), field, repetition);
          if (ParquetUtils.hasFieldId(field)) {
            return type.withId(ParquetUtils.getFieldId(field));
          }
          return type;
        }).toArray(Type[]::new))
        .named("spark_schema");
  }

  private Type convertField(Schema avroFieldSchema, StructField structField, Type.Repetition repetition) {
    Schema resolvedSchema = avroFieldSchema == null ? null : resolveNullableSchema(avroFieldSchema);
    LogicalType logicalType = resolvedSchema != null ? resolvedSchema.getLogicalType() : null;

    DataType dataType = structField.dataType();
    if (dataType == DataTypes.BooleanType) {
      return Types.primitive(BOOLEAN, repetition).named(structField.name());
    } else if (dataType == DataTypes.DateType) {
      return Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.dateType()).named(structField.name());
    } else if (dataType == DataTypes.ShortType) {
      return Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(16, true)).named(structField.name());
    } else if (dataType == DataTypes.IntegerType) {
      return Types.primitive(INT32, repetition).named(structField.name());
    } else if (dataType == DataTypes.LongType) {
      return Types.primitive(INT64, repetition).named(structField.name());
    } else if (dataType == DataTypes.TimestampType) {
      if (logicalType == null || logicalType.getName().equals(LogicalTypes.timestampMicros().getName())) {
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
      } else if (logicalType.getName().equals(LogicalTypes.timestampMillis().getName())) {
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(structField.name());
      } else {
        throw new UnsupportedOperationException("Unsupported timestamp type: " + logicalType);
      }
    } else if (dataType == DataTypes.TimestampNTZType) {
      if (logicalType == null || logicalType.getName().equals(LogicalTypes.localTimestampMicros().getName())) {
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
      } else if (logicalType.getName().equals(LogicalTypes.localTimestampMillis().getName())) {
        return Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named(structField.name());
      } else {
        throw new UnsupportedOperationException("Unsupported timestamp type: " + logicalType);
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
    } else if (dataType instanceof DecimalType) {
      int precision = ((DecimalType) dataType).precision();
      int scale = ((DecimalType) dataType).scale();
      return Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .length(Decimal.minBytesForPrecision()[precision])
          .named(structField.name());
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType elementType = arrayType.elementType();
      Schema avroElementSchema = resolvedSchema.getElementType();
      if (!writeLegacyListFormat) {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            .addField(
                Types.repeatedGroup()
                    .addField(convertField(avroElementSchema, new StructField("element", elementType, arrayType.containsNull(), Metadata.empty()),
                        arrayType.containsNull() ? OPTIONAL : REQUIRED))
                    .named("list"))
            .named(structField.name());
      } else if ((arrayType.containsNull())) {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            .addField(Types
                .buildGroup(REPEATED)
                // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
                .addField(convertField(avroElementSchema, new StructField("array", elementType, true, Metadata.empty()), OPTIONAL))
                .named("bag"))
            .named(structField.name());
      } else {
        return Types
            .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
            // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
            .addField(convertField(avroElementSchema, new StructField("array", elementType, false, Metadata.empty()), REPEATED))
            .named(structField.name());
      }
    } else if (dataType instanceof MapType) {
      Schema avroValueSchema = resolvedSchema == null ? null : resolvedSchema.getValueType();
      MapType mapType = (MapType) dataType;
      return Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.mapType())
          .addField(
              Types
                  .repeatedGroup()
                  .addField(convertField(MAP_KEY_SCHEMA, new StructField(MAP_KEY_NAME, DataTypes.StringType, false, Metadata.empty()), REQUIRED))
                  .addField(convertField(avroValueSchema, new StructField(MAP_VALUE_NAME, mapType.valueType(), mapType.valueContainsNull(), Metadata.empty()),
                      mapType.valueContainsNull() ? OPTIONAL : REQUIRED))
                  .named(MAP_REPEATED_NAME))
          .named(structField.name());
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }
}
