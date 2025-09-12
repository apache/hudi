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
import org.apache.hudi.SparkAdapterSupport;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.VersionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import scala.Enumeration;
import scala.Function1;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_FIELD_ID_WRITE_ENABLED;

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
  private final Enumeration.Value datetimeRebaseMode = (Enumeration.Value) SparkAdapterSupport.getSparkAdapter().getDateTimeRebaseMode();
  private final Function1<Object, Object> dateRebaseFunction = DataSourceUtils.createDateRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private final Function1<Object, Object> timestampRebaseFunction = DataSourceUtils.createTimestampRebaseFuncInWrite(datetimeRebaseMode, "Parquet");
  private RecordConsumer recordConsumer;
  private final boolean writeLegacyListFormat;
  private final ValueWriter[] rootFieldWriters;
  private final Schema avroSchema;

  public HoodieRowParquetWriteSupport(Configuration conf, StructType schema, Schema avroSchema, Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    Configuration hadoopConf = new Configuration(conf);
    String writeLegacyFormatEnabled = config.getStringOrDefault(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED, "false");
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", writeLegacyFormatEnabled);
    hadoopConf.set("spark.sql.parquet.outputTimestampType", config.getStringOrDefault(HoodieStorageConfig.PARQUET_OUTPUT_TIMESTAMP_TYPE));
    hadoopConf.set("spark.sql.parquet.fieldId.write.enabled", config.getStringOrDefault(PARQUET_FIELD_ID_WRITE_ENABLED));
    this.writeLegacyListFormat = Boolean.parseBoolean(writeLegacyFormatEnabled)
        || Boolean.parseBoolean(config.getStringOrDefault(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false"));
    ParquetWriteSupport.setSchema(HoodieInternalRowUtils.getCachedSchema(avroSchema), hadoopConf);
    this.avroSchema = avroSchema;
    this.rootFieldWriters = getFieldWriters(schema, avroSchema);
    this.hadoopConf = hadoopConf;
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new);
  }

  private ValueWriter[] getFieldWriters(StructType schema, Schema avroSchema) {
    Map<String, Integer> fieldNameToIndex = new HashMap<>();
    for (int i = 0; i < schema.fields().length; i++) {
      fieldNameToIndex.put(schema.fields()[i].name(), i);
    }
    return IntStream.range(0, avroSchema.getFields().size()).mapToObj(i -> {
      Schema.Field field = avroSchema.getFields().get(i);
      Integer structIndex = fieldNameToIndex.get(field.name());
      return makeWriter(field.schema(), schema.fields()[structIndex].dataType());
    }).toArray(ValueWriter[]::new);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("org.apache.spark.version", VersionUtils.shortVersion(HoodieSparkUtils.getSparkVersion()));
    if (SparkAdapterSupport.getSparkAdapter().isLegacyBehaviorPolicy(datetimeRebaseMode)) {
      metadata.put("org.apache.spark.legacyDateTime", "");
      metadata.put("org.apache.spark.timeZone", SQLConf.get().sessionLocalTimeZone());
    }
    Configuration configurationCopy = new Configuration(configuration);
    configurationCopy.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, Boolean.toString(writeLegacyListFormat));
    MessageType messageType = new AvroSchemaConverter(configurationCopy).convert(avroSchema);
    return new WriteContext(messageType, metadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(InternalRow row) {
    consumeMessage(() -> writeFields(row, avroSchema, rootFieldWriters));
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

  private void writeFields(InternalRow row, Schema schema, ValueWriter[] fieldWriters) {
    for (int i = 0; i < fieldWriters.length; i++) {
      int index = i;
      if (!row.isNullAt(i)) {
        Schema.Field field = schema.getFields().get(index);
        consumeField(field.name(), index, () -> fieldWriters[index].write(row, index));
      }
    }
  }

  private ValueWriter makeWriter(Schema avroSchema, DataType dataType) {
    Schema resolvedSchema = resolveNullableSchema(avroSchema);
    Schema.Type type = resolvedSchema.getType();
    LogicalType logicalType = resolvedSchema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        return (row, ordinal) -> recordConsumer.addBoolean(row.getBoolean(ordinal));
      case INT:
        if (logicalType != null) {
          if (logicalType.getName().equals(LogicalTypes.date().getName())) {
            return (row, ordinal) -> recordConsumer.addInteger((Integer) dateRebaseFunction.apply(row.getInt(ordinal)));
          }
        }
        return (row, ordinal) -> recordConsumer.addInteger(row.getInt(ordinal));
      case LONG:
        if (logicalType != null) {
          if (logicalType.getName().equals(LogicalTypes.timestampMillis().getName())) {
            return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis((long) timestampRebaseFunction.apply(row.getLong(ordinal))));
          } else if (logicalType.getName().equals(LogicalTypes.timestampMicros().getName())) {
            return (row, ordinal) -> recordConsumer.addLong((long) timestampRebaseFunction.apply(row.getLong(ordinal)));
          } else if (logicalType.getName().equals(LogicalTypes.localTimestampMicros().getName())) {
            return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
          } else if (logicalType.getName().equals(LogicalTypes.localTimestampMillis().getName()) && dataType instanceof TimestampNTZType) {
            // only go through conversion if spark data type is TimestampNTZType
            return (row, ordinal) -> recordConsumer.addLong(DateTimeUtils.microsToMillis(row.getLong(ordinal)));
          }
        }
        return (row, ordinal) -> recordConsumer.addLong(row.getLong(ordinal));
      case FLOAT:
        return (row, ordinal) -> recordConsumer.addFloat(row.getFloat(ordinal));
      case DOUBLE:
        return (row, ordinal) -> recordConsumer.addDouble(row.getDouble(ordinal));
      case STRING:
      case ENUM:
        return (row, ordinal) -> recordConsumer.addBinary(
            Binary.fromReusedByteArray(row.getUTF8String(ordinal).getBytes()));
      case BYTES:
      case FIXED:
        if (logicalType != null && logicalType.getName().equals("decimal")) {
          return (row, ordinal) -> {
            int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
            int scale = ((LogicalTypes.Decimal) logicalType).getScale();
            long unscaled = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
            int i = 0;
            int numBytes = Decimal.minBytesForPrecision()[precision];
            int shift = 8 * (numBytes - 1);
            while (i < numBytes) {
              decimalBuffer[i] = (byte) ((unscaled >> shift) & 0xFF);
              i += 1;
              shift -= 8;
            }
            recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes));
          };
        }
        return (row, ordinal) -> recordConsumer.addBinary(
            Binary.fromReusedByteArray(row.getBinary(ordinal)));
      case RECORD:
        ValueWriter[] fieldWriters = getFieldWriters(((StructType) dataType), resolvedSchema);
        return (row, ordinal) ->
          consumeGroup(() -> writeFields(row.getStruct(ordinal, resolvedSchema.getFields().size()), resolvedSchema, fieldWriters));
      case ARRAY:
        ValueWriter elementWriter = makeWriter(resolvedSchema.getElementType(), ((ArrayType) dataType).elementType());
        if (!writeLegacyListFormat) {
          return threeLevelArrayWriter("list", "element", elementWriter);
        } else if (resolvedSchema.getElementType().isNullable()) {
          return threeLevelArrayWriter("bag", "array", elementWriter);
        } else {
          return twoLevelArrayWriter("array", elementWriter);
        }
      case MAP:
        ValueWriter keyWriter = makeWriter(MAP_KEY_SCHEMA, DataTypes.StringType);
        ValueWriter valueWriter = makeWriter(resolvedSchema.getValueType(), ((MapType) dataType).valueType());
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
                    if (!keyArray.isNullAt(index)) {
                      consumeField(MAP_KEY_NAME, 0, () -> keyWriter.write(keyArray, index));
                    }
                    if (!valueArray.isNullAt(index)) {
                      consumeField(MAP_VALUE_NAME, 1, () -> valueWriter.write(valueArray, index));
                    }
                  });
                }
              });
            }
          });
        };
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
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

  public static HoodieRowParquetWriteSupport getHoodieRowParquetWriteSupport(Configuration conf, StructType schema, Schema avroSchema,
                                                                             Option<BloomFilter> bloomFilterOpt, HoodieConfig config) {
    return (HoodieRowParquetWriteSupport) ReflectionUtils.loadClass(
        config.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS),
        new Class<?>[] {Configuration.class, StructType.class, Schema.class, Option.class, HoodieConfig.class},
        conf, schema, avroSchema, bloomFilterOpt, config);
  }

}
