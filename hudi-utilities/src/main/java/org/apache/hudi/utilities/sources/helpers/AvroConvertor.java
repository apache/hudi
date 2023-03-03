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

import com.google.protobuf.Message;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;

import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import static org.apache.hudi.utilities.sources.helpers.SanitizationUtils.Config.SANITIZE_SCHEMA_FIELD_NAMES;
import static org.apache.hudi.utilities.sources.helpers.SanitizationUtils.Config.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;

/**
 * Convert a variety of datum into Avro GenericRecords. Has a bunch of lazy fields to circumvent issues around
 * serializing these objects from driver to executors
 */
public class AvroConvertor implements Serializable {

  private static final long serialVersionUID = 1L;
  /**
   * To be lazily inited on executors.
   */
  private transient Schema schema;

  private final String schemaStr;
  private final String invalidCharMask;
  private final boolean shouldSanitize;

  /**
   * To be lazily inited on executors.
   */
  private transient MercifulJsonConverter jsonConverter;


  /**
   * To be lazily inited on executors.
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

  public AvroConvertor(Schema schema) {
    this(schema, SANITIZE_SCHEMA_FIELD_NAMES.defaultValue(), SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  public AvroConvertor(Schema schema, boolean shouldSanitize, String invalidCharMask) {
    this.schemaStr = schema.toString();
    this.schema = schema;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  private void initSchema() {
    if (schema == null) {
      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse(schemaStr);
    }
  }

  private void initInjection() {
    if (recordInjection == null) {
      recordInjection = GenericAvroCodecs.toBinary(schema);
    }
  }

  private void initJsonConvertor() {
    if (jsonConverter == null) {
      jsonConverter = new MercifulJsonConverter(this.shouldSanitize, this.invalidCharMask);
    }
  }

  public GenericRecord fromJson(String json) {
    initSchema();
    initJsonConvertor();
    return jsonConverter.convert(json, schema);
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

  public Schema getSchema() {
    return new Schema.Parser().parse(schemaStr);
  }

  public GenericRecord fromAvroBinary(byte[] avroBinary) {
    initSchema();
    initInjection();
    return recordInjection.invert(avroBinary).get();
  }

  public GenericRecord fromProtoMessage(Message message) {
    initSchema();
    return ProtoConversionUtil.convertToAvro(schema, message);
  }

  /**
   * this.schema is required to have kafka offsets for this to work
   */
  public GenericRecord withKafkaFieldsAppended(ConsumerRecord consumerRecord) {
    initSchema();
    GenericRecord record = (GenericRecord) consumerRecord.value();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(this.schema);
    for (Schema.Field field :  record.getSchema().getFields()) {
      recordBuilder.set(field, record.get(field.name()));
    }
    recordBuilder.set(KAFKA_SOURCE_OFFSET_COLUMN, consumerRecord.offset());
    recordBuilder.set(KAFKA_SOURCE_PARTITION_COLUMN, consumerRecord.partition());
    recordBuilder.set(KAFKA_SOURCE_TIMESTAMP_COLUMN, consumerRecord.timestamp());
    return recordBuilder.build();
  }

}
