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
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

/**
 * Adapts data-format provided by the source to the data-format required by the client (DeltaStreamer).
 */
public final class SourceFormatAdapter implements Closeable {

  private final Source source;

  public SourceFormatAdapter(Source source) {
    this.source = source;
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
        InputBatch<Dataset<Row>> r = ((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit);
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
        return ((Source<Dataset<Row>>) source).fetchNext(lastCkptStr, sourceLimit);
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
