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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import com.google.protobuf.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

/**
 * Adapts data-format provided by the source to the data-format required by the client (DeltaStreamer).
 */
public final class SourceFormatAdapter implements Closeable {

  public static class SourceFormatAdapterConfig extends HoodieConfig {
    // sanitizes invalid columns both in the data read from source and also in the schema.
    // invalid definition here goes by avro naming convention (https://avro.apache.org/docs/current/spec.html#names).
    public static final ConfigProperty<Boolean> SANITIZE_AVRO_FIELD_NAMES = ConfigProperty
        .key("hoodie.deltastreamer.source.sanitize.invalid.column.names")
        .defaultValue(false)
        .withDocumentation("Sanitizes invalid column names both in the data and also in the schema");

    public static final ConfigProperty<String> AVRO_FIELD_NAME_INVALID_CHAR_MASK = ConfigProperty
        .key("hoodie.deltastreamer.source.sanitize.invalid.char.mask")
        .defaultValue("__")
        .withDocumentation("Character mask to be used as replacement for invalid field names");

    public SourceFormatAdapterConfig() {
      super();
    }

    public SourceFormatAdapterConfig(TypedProperties props) {
      super(props);
    }
  }

  private final Source source;
  private final SourceFormatAdapterConfig config;

  public SourceFormatAdapter(Source source) {
    this(source, Option.empty());
  }

  public SourceFormatAdapter(Source source,
                             Option<TypedProperties> props) {
    this.source = source;
    this.config = props.isPresent() ? new SourceFormatAdapterConfig(props.get()) : new SourceFormatAdapterConfig();
  }

  /**
   * Config that automatically sanitizes the field names as per avro naming rules.
   * @return enabled status.
   */
  private boolean isNameSanitizingEnabled() {
    return config.getBooleanOrDefault(SourceFormatAdapterConfig.SANITIZE_AVRO_FIELD_NAMES);
  }

  /**
   * Replacement mask for invalid characters encountered in avro names.
   * @return sanitized value.
   */
  private String getInvalidCharMask() {
    return config.getStringOrDefault(SourceFormatAdapterConfig.AVRO_FIELD_NAME_INVALID_CHAR_MASK);
  }

  private static DataType sanitizeDataTypeForAvro(DataType dataType, String invalidCharMask) {
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType sanitizedDataType = sanitizeDataTypeForAvro(arrayType.elementType(), invalidCharMask);
      return new ArrayType(sanitizedDataType, arrayType.containsNull());
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType sanitizedKeyDataType = sanitizeDataTypeForAvro(mapType.keyType(), invalidCharMask);
      DataType sanitizedValueDataType = sanitizeDataTypeForAvro(mapType.valueType(), invalidCharMask);
      return new MapType(sanitizedKeyDataType, sanitizedValueDataType, mapType.valueContainsNull());
    } else if (dataType instanceof StructType) {
      return sanitizeStructTypeForAvro((StructType) dataType, invalidCharMask);
    }
    return dataType;
  }

  // TODO: Rebase this to use InternalSchema when it is ready.
  private static StructType sanitizeStructTypeForAvro(StructType structType, String invalidCharMask) {
    StructType sanitizedStructType = new StructType();
    StructField[] structFields = structType.fields();
    for (StructField s : structFields) {
      DataType currFieldDataTypeSanitized = sanitizeDataTypeForAvro(s.dataType(), invalidCharMask);
      StructField structFieldCopy = new StructField(HoodieAvroUtils.sanitizeName(s.name(), invalidCharMask),
          currFieldDataTypeSanitized, s.nullable(), s.metadata());
      sanitizedStructType = sanitizedStructType.add(structFieldCopy);
    }
    return sanitizedStructType;
  }

  private static Dataset<Row> sanitizeColumnNamesForAvro(Dataset<Row> inputDataset, String invalidCharMask) {
    StructField[] inputFields = inputDataset.schema().fields();
    Dataset<Row> targetDataset = inputDataset;
    for (StructField sf : inputFields) {
      DataType sanitizedFieldDataType = sanitizeDataTypeForAvro(sf.dataType(), invalidCharMask);
      if (!sanitizedFieldDataType.equals(sf.dataType())) {
        // Sanitizing column names for nested types can be thought of as going from one schema to another
        // which are structurally similar except for actual column names itself. So casting is safe and sufficient.
        targetDataset = targetDataset.withColumn(sf.name(), targetDataset.col(sf.name()).cast(sanitizedFieldDataType));
      }
      String possibleRename = HoodieAvroUtils.sanitizeName(sf.name(), invalidCharMask);
      if (!sf.name().equals(possibleRename)) {
        targetDataset = targetDataset.withColumnRenamed(sf.name(), possibleRename);
      }
    }
    return targetDataset;
  }

  /**
   * Sanitize all columns including nested ones as per Avro conventions.
   * @param srcBatch
   * @param shouldSanitize
   * @param invalidCharMask
   * @return sanitized batch.
   */
  private static InputBatch<Dataset<Row>> trySanitizeFieldNames(InputBatch<Dataset<Row>> srcBatch,
                                                                boolean shouldSanitize,
                                                                String invalidCharMask) {
    if (!shouldSanitize || !srcBatch.getBatch().isPresent()) {
      return srcBatch;
    }
    Dataset<Row> srcDs = srcBatch.getBatch().get();
    Dataset<Row> targetDs = sanitizeColumnNamesForAvro(srcDs, invalidCharMask);
    return new InputBatch<>(Option.ofNullable(targetDs), srcBatch.getCheckpointForNextBatch(), srcBatch.getSchemaProvider());
  }

  /**
   * Fetch new data in avro format. If the source provides data in different format, they are translated to Avro format
   */
  public InputBatch<JavaRDD<GenericRecord>> fetchNewDataInAvroFormat(Option<String> lastCkptStr, long sourceLimit) {
    switch (source.getSourceType()) {
      case AVRO:
        return ((Source<JavaRDD<GenericRecord>>) source).fetchNext(lastCkptStr, sourceLimit);
      case JSON: {
        InputBatch<JavaRDD<String>> r = ((Source<JavaRDD<String>>) source).fetchNext(lastCkptStr, sourceLimit);
        AvroConvertor convertor = new AvroConvertor(r.getSchemaProvider().getSourceSchema());
        return new InputBatch<>(Option.ofNullable(r.getBatch().map(rdd -> rdd.map(convertor::fromJson)).orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      case ROW: {
        InputBatch<Dataset<Row>> r = trySanitizeFieldNames(((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit),
            isNameSanitizingEnabled(), getInvalidCharMask());
        return new InputBatch<>(Option.ofNullable(r.getBatch().map(
            rdd -> {
              SchemaProvider originalProvider = UtilHelpers.getOriginalSchemaProvider(r.getSchemaProvider());
              return (originalProvider instanceof FilebasedSchemaProvider)
                  // If the source schema is specified through Avro schema,
                  // pass in the schema for the Row-to-Avro conversion
                  // to avoid nullability mismatch between Avro schema and Row schema
                  ? HoodieSparkUtils.createRdd(rdd, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, true,
                  org.apache.hudi.common.util.Option.ofNullable(r.getSchemaProvider().getSourceSchema())
              ).toJavaRDD() : HoodieSparkUtils.createRdd(rdd,
                  HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, false, Option.empty()).toJavaRDD();
            })
            .orElse(null)), r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      case PROTO: {
        InputBatch<JavaRDD<Message>> r = ((Source<JavaRDD<Message>>) source).fetchNext(lastCkptStr, sourceLimit);
        AvroConvertor convertor = new AvroConvertor(r.getSchemaProvider().getSourceSchema());
        return new InputBatch<>(Option.ofNullable(r.getBatch().map(rdd -> rdd.map(convertor::fromProtoMessage)).orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      default:
        throw new IllegalArgumentException("Unknown source type (" + source.getSourceType() + ")");
    }
  }

  /**
   * Fetch new data in row format. If the source provides data in different format, they are translated to Row format
   */
  public InputBatch<Dataset<Row>> fetchNewDataInRowFormat(Option<String> lastCkptStr, long sourceLimit) {
    switch (source.getSourceType()) {
      case ROW:
        return trySanitizeFieldNames(((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit),
            isNameSanitizingEnabled(), getInvalidCharMask());
      case AVRO: {
        InputBatch<JavaRDD<GenericRecord>> r = ((Source<JavaRDD<GenericRecord>>) source).fetchNext(lastCkptStr, sourceLimit);
        Schema sourceSchema = r.getSchemaProvider().getSourceSchema();
        return new InputBatch<>(
            Option
                .ofNullable(
                    r.getBatch()
                        .map(rdd -> AvroConversionUtils.createDataFrame(JavaRDD.toRDD(rdd), sourceSchema.toString(),
                            source.getSparkSession())
                        )
                        .orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      case JSON: {
        InputBatch<JavaRDD<String>> r = ((Source<JavaRDD<String>>) source).fetchNext(lastCkptStr, sourceLimit);
        Schema sourceSchema = r.getSchemaProvider().getSourceSchema();
        StructType dataType = AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema);
        return new InputBatch<>(
            Option.ofNullable(
                r.getBatch().map(rdd -> source.getSparkSession().read().schema(dataType).json(rdd)).orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      case PROTO: {
        InputBatch<JavaRDD<Message>> r = ((Source<JavaRDD<Message>>) source).fetchNext(lastCkptStr, sourceLimit);
        Schema sourceSchema = r.getSchemaProvider().getSourceSchema();
        AvroConvertor convertor = new AvroConvertor(r.getSchemaProvider().getSourceSchema());
        return new InputBatch<>(
            Option
                .ofNullable(
                    r.getBatch()
                        .map(rdd -> rdd.map(convertor::fromProtoMessage))
                        .map(rdd -> AvroConversionUtils.createDataFrame(JavaRDD.toRDD(rdd), sourceSchema.toString(),
                            source.getSparkSession())
                        )
                        .orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      default:
        throw new IllegalArgumentException("Unknown source type (" + source.getSourceType() + ")");
    }
  }

  public Source getSource() {
    return source;
  }

  @Override
  public void close() {
    if (source instanceof Closeable) {
      try {
        ((Closeable) source).close();
      } catch (IOException e) {
        throw new HoodieIOException(String.format("Failed to shutdown the source (%s)", source.getClass().getName()), e);
      }
    }
  }
}
