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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import com.google.protobuf.Message;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Arrays;

import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_KEY_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;

/**
 * Convert a variety of datum into Avro GenericRecords. Has a bunch of lazy fields to circumvent issues around
 * serializing these objects from driver to executors
 */
public class AvroConvertor implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Serializable string representation of the schema.
   */
  private final String schemaStr;

  /**
   * To be lazily initialized on executors.
   */
  private transient HoodieSchema schema;

  private final String invalidCharMask;
  private final boolean shouldSanitize;

  /**
   * To be lazily initialized on executors.
   */
  private transient MercifulJsonConverter jsonConverter;

  /**
   * To be lazily initialized on executors.
   */
  private transient Injection<GenericRecord, byte[]> recordInjection;

  public AvroConvertor(String schemaStr) {
    this(schemaStr, SANITIZE_SCHEMA_FIELD_NAMES.defaultValue(), SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  public AvroConvertor(String schemaStr, boolean shouldSanitize, String invalidCharMask) {
    this.schemaStr = schemaStr;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  public AvroConvertor(HoodieSchema hoodieSchema) {
    this(hoodieSchema, SANITIZE_SCHEMA_FIELD_NAMES.defaultValue(), SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  public AvroConvertor(HoodieSchema hoodieSchema, boolean shouldSanitize, String invalidCharMask) {
    this.schemaStr = hoodieSchema.toString();
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  private void initSchema() {
    if (schema == null) {
      schema = HoodieSchema.parse(schemaStr);
    }
  }

  private void initInjection() {
    if (recordInjection == null) {
      recordInjection = GenericAvroCodecs.toBinary(schema.toAvroSchema());
    }
  }

  private void initJsonConvertor() {
    if (jsonConverter == null) {
      jsonConverter = new MercifulJsonConverter(this.shouldSanitize, this.invalidCharMask);
    }
  }

  public GenericRecord fromJson(String json) {
    try {
      initSchema();
      initJsonConvertor();
      return jsonConverter.convert(json, schema);
    } catch (Exception e) {
      String errorMessage = "Failed to convert JSON string to Avro record: ";
      if (json != null) {
        throw new HoodieSchemaException(errorMessage + json + "; schema: " + schema, e);
      } else {
        throw new HoodieSchemaException(errorMessage + "JSON string was null.", e);
      }
    }
  }

  public Either<GenericRecord,String> fromJsonWithError(String json) {
    GenericRecord genericRecord;
    try {
      genericRecord = fromJson(json);
    } catch (Exception e) {
      return new Right(json);
    }
    return new Left(genericRecord);
  }

  public HoodieSchema getSchema() {
    initSchema();
    return schema;
  }

  public GenericRecord fromAvroBinary(byte[] avroBinary) {
    try {
      initSchema();
      initInjection();
      return recordInjection.invert(avroBinary).get();
    } catch (Exception e) {
      if (avroBinary != null) {
        throw new HoodieSchemaException("Failed to get avro schema from avro binary: " + Arrays.toString(avroBinary), e);
      } else {
        throw new HoodieSchemaException("Failed to get avro schema from avro binary. Binary is null", e);
      }
    }

  }

  public GenericRecord fromProtoMessage(Message message) {
    try {
      initSchema();
      return ProtoConversionUtil.convertToAvro(schema, message);
    } catch (Exception e) {
      throw new HoodieSchemaException("Failed to get avro schema from proto message", e);
    }
  }

  /**
   * this.schema is required to have kafka offsets for this to work
   */
  public GenericRecord withKafkaFieldsAppended(ConsumerRecord consumerRecord) {
    initSchema();
    GenericRecord recordValue = (GenericRecord) consumerRecord.value();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(this.schema.toAvroSchema());
    recordValue.getSchema().getFields().forEach(field -> {
      recordBuilder.set(field, recordValue.get(field.name()));
    });
    String recordKey = StringUtils.objToString(consumerRecord.key());
    recordBuilder.set(KAFKA_SOURCE_OFFSET_COLUMN, consumerRecord.offset());
    recordBuilder.set(KAFKA_SOURCE_PARTITION_COLUMN, consumerRecord.partition());
    recordBuilder.set(KAFKA_SOURCE_TIMESTAMP_COLUMN, consumerRecord.timestamp());
    recordBuilder.set(KAFKA_SOURCE_KEY_COLUMN, recordKey);
    return recordBuilder.build();
  }

}
