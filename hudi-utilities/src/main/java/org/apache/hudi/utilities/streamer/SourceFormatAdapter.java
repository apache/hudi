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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

import com.google.protobuf.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import scala.util.Either;

import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;
import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

/**
 * Adapts data-format provided by the source to the data-format required by the client (DeltaStreamer).
 */
public final class SourceFormatAdapter implements Closeable {

  private final Source source;
  private boolean shouldSanitize = SANITIZE_SCHEMA_FIELD_NAMES.defaultValue();
  private String invalidCharMask = SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue();

  private Option<BaseErrorTableWriter> errorTableWriter = Option.empty();

  public SourceFormatAdapter(Source source) {
    this(source, Option.empty(), Option.empty());
  }

  public SourceFormatAdapter(Source source, Option<BaseErrorTableWriter> errorTableWriter, Option<TypedProperties> props) {
    this.source = source;
    this.errorTableWriter = errorTableWriter;
    if (props.isPresent()) {
      this.shouldSanitize = SanitizationUtils.getShouldSanitize(props.get());
      this.invalidCharMask = SanitizationUtils.getInvalidCharMask(props.get());
    }
    if (this.shouldSanitize && source.getSourceType() == Source.SourceType.PROTO) {
      throw new IllegalArgumentException("PROTO cannot be sanitized");
    }
  }

  /**
   * Config that automatically sanitizes the field names as per avro naming rules.
   * @return enabled status.
   */
  private boolean isFieldNameSanitizingEnabled() {
    return shouldSanitize;
  }

  /**
   * Replacement mask for invalid characters encountered in avro names.
   * @return sanitized value.
   */
  private String getInvalidCharMask() {
    return invalidCharMask;
  }

  /**
   * Sanitize all columns including nested ones as per Avro conventions.
   * @param srcBatch
   * @return sanitized batch.
   */
  private InputBatch<Dataset<Row>> maybeSanitizeFieldNames(InputBatch<Dataset<Row>> srcBatch) {
    if (!isFieldNameSanitizingEnabled() || !srcBatch.getBatch().isPresent()) {
      return srcBatch;
    }
    Dataset<Row> srcDs = srcBatch.getBatch().get();
    Dataset<Row> targetDs = SanitizationUtils.sanitizeColumnNamesForAvro(srcDs, getInvalidCharMask());
    return new InputBatch<>(Option.ofNullable(targetDs), srcBatch.getCheckpointForNextBatch(), srcBatch.getSchemaProvider());
  }

  /**
   * transform input rdd of json string to generic records with support for adding error events to error table
   * @param inputBatch
   * @return
   */
  private JavaRDD<GenericRecord> transformJsonToGenericRdd(InputBatch<JavaRDD<String>> inputBatch) {
    MercifulJsonConverter.clearCache(inputBatch.getSchemaProvider().getSourceSchema().getFullName());
    AvroConvertor convertor = new AvroConvertor(inputBatch.getSchemaProvider().getSourceSchema(), isFieldNameSanitizingEnabled(), getInvalidCharMask());
    return inputBatch.getBatch().map(rdd -> {
      if (errorTableWriter.isPresent()) {
        JavaRDD<Either<GenericRecord,String>> javaRDD = rdd.map(convertor::fromJsonWithError);
        errorTableWriter.get().addErrorEvents(javaRDD.filter(x -> x.isRight()).map(x ->
            new ErrorEvent<>(x.right().get(), ErrorEvent.ErrorReason.JSON_AVRO_DESERIALIZATION_FAILURE)));
        return javaRDD.filter(x -> x.isLeft()).map(x -> x.left().get());
      } else {
        return rdd.map(convertor::fromJson);
      }
    }).orElse(null);
  }

  /**
   * transform datasets with error events when error table is enabled
   * @param eventsRow
   * @return
   */
  public Option<Dataset<Row>> processErrorEvents(Option<Dataset<Row>> eventsRow,
                                                      ErrorEvent.ErrorReason errorReason) {
    return eventsRow.map(dataset -> {
          if (errorTableWriter.isPresent() && Arrays.stream(dataset.columns()).collect(Collectors.toList())
              .contains(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)) {
            errorTableWriter.get().addErrorEvents(dataset.filter(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME).isNotNull())
                .select(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)).toJavaRDD().map(ev ->
                    new ErrorEvent<>(ev.getString(0), errorReason)));
            return dataset.filter(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME).isNull()).drop(ERROR_TABLE_CURRUPT_RECORD_COL_NAME);
          }
          return dataset;
        }
    );
  }

  /**
   * Fetch new data in avro format. If the source provides data in different format, they are translated to Avro format
   */
  public InputBatch<JavaRDD<GenericRecord>> fetchNewDataInAvroFormat(Option<String> lastCkptStr, long sourceLimit) {
    switch (source.getSourceType()) {
      case AVRO:
        //don't need to sanitize because it's already avro
        return ((Source<JavaRDD<GenericRecord>>) source).fetchNext(lastCkptStr, sourceLimit);
      case JSON: {
        //sanitizing is done inside the convertor in transformJsonToGenericRdd if enabled
        InputBatch<JavaRDD<String>> r = ((Source<JavaRDD<String>>) source).fetchNext(lastCkptStr, sourceLimit);
        JavaRDD<GenericRecord> eventsRdd = transformJsonToGenericRdd(r);
        return new InputBatch<>(Option.ofNullable(eventsRdd),r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      case ROW: {
        //we do the sanitizing here if enabled
        InputBatch<Dataset<Row>> r = maybeSanitizeFieldNames(((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit));
        return new InputBatch<>(Option.ofNullable(r.getBatch().map(
            rdd -> {
                SchemaProvider originalProvider = UtilHelpers.getOriginalSchemaProvider(r.getSchemaProvider());
                return (originalProvider instanceof FilebasedSchemaProvider || (originalProvider instanceof SchemaRegistryProvider))
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
        //TODO([HUDI-5830]) implement field name sanitization
        InputBatch<JavaRDD<Message>> r = ((Source<JavaRDD<Message>>) source).fetchNext(lastCkptStr, sourceLimit);
        AvroConvertor convertor = new AvroConvertor(r.getSchemaProvider().getSourceSchema());
        return new InputBatch<>(Option.ofNullable(r.getBatch().map(rdd -> rdd.map(convertor::fromProtoMessage)).orElse(null)),
            r.getCheckpointForNextBatch(), r.getSchemaProvider());
      }
      default:
        throw new IllegalArgumentException("Unknown source type (" + source.getSourceType() + ")");
    }
  }

  private InputBatch<Dataset<Row>> avroDataInRowFormat(InputBatch<JavaRDD<GenericRecord>> r) {
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

  /**
   * Fetch new data in row format. If the source provides data in different format, they are translated to Row format
   */
  public InputBatch<Dataset<Row>> fetchNewDataInRowFormat(Option<String> lastCkptStr, long sourceLimit) {
    switch (source.getSourceType()) {
      case ROW:
        //we do the sanitizing here if enabled
        InputBatch<Dataset<Row>> datasetInputBatch = maybeSanitizeFieldNames(((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit));
        return new InputBatch<>(processErrorEvents(datasetInputBatch.getBatch(),
            ErrorEvent.ErrorReason.JSON_ROW_DESERIALIZATION_FAILURE),
            datasetInputBatch.getCheckpointForNextBatch(), datasetInputBatch.getSchemaProvider());
      case AVRO: {
        //don't need to sanitize because it's already avro
        InputBatch<JavaRDD<GenericRecord>> r = ((Source<JavaRDD<GenericRecord>>) source).fetchNext(lastCkptStr, sourceLimit);
        return avroDataInRowFormat(r);
      }
      case JSON: {
        if (isFieldNameSanitizingEnabled()) {
          //leverage the json -> avro sanitizing. TODO([HUDI-5829]) Optimize by sanitizing during direct conversion
          InputBatch<JavaRDD<GenericRecord>> r = fetchNewDataInAvroFormat(lastCkptStr, sourceLimit);
          return avroDataInRowFormat(r);

        }
        InputBatch<JavaRDD<String>> r = ((Source<JavaRDD<String>>) source).fetchNext(lastCkptStr, sourceLimit);
        Schema sourceSchema = r.getSchemaProvider().getSourceSchema();
        if (errorTableWriter.isPresent()) {
          // if error table writer is enabled, during spark read `columnNameOfCorruptRecord` option is configured.
          // Any records which spark is unable to read successfully are transferred to the column
          // configured via this option. The column is then used to trigger error events.
          StructType dataType = AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema)
              .add(new StructField(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, DataTypes.StringType, true, Metadata.empty()));
          Option<Dataset<Row>> dataset = r.getBatch().map(rdd -> source.getSparkSession().read()
              .option("columnNameOfCorruptRecord", ERROR_TABLE_CURRUPT_RECORD_COL_NAME).schema(dataType.asNullable())
              .json(rdd));
          Option<Dataset<Row>> eventsDataset = processErrorEvents(dataset,
              ErrorEvent.ErrorReason.JSON_ROW_DESERIALIZATION_FAILURE);
          return new InputBatch<>(
              eventsDataset,
              r.getCheckpointForNextBatch(), r.getSchemaProvider());
        } else {
          StructType dataType = AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema);
          return new InputBatch<>(
              Option.ofNullable(
                  r.getBatch().map(rdd -> source.getSparkSession().read().schema(dataType).json(rdd)).orElse(null)),
              r.getCheckpointForNextBatch(), r.getSchemaProvider());
        }
      }
      case PROTO: {
        //TODO([HUDI-5830]) implement field name sanitization
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
