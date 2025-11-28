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

package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Base Hudi Writer that manages reading the raw Kafka records and
 * converting them to {@link HoodieRecord}s that can be written to Hudi by
 * the derived implementations of this class.
 */
public abstract class AbstractConnectWriter implements ConnectWriter<WriteStatus> {

  public static final String KAFKA_AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
  public static final String KAFKA_JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
  public static final String KAFKA_STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";
  private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectWriter.class);
  protected final String instantTime;

  private final KeyGenerator keyGenerator;
  private final SchemaProvider schemaProvider;
  protected final KafkaConnectConfigs connectConfigs;

  public AbstractConnectWriter(KafkaConnectConfigs connectConfigs,
                               KeyGenerator keyGenerator,
                               SchemaProvider schemaProvider, String instantTime) {
    this.connectConfigs = connectConfigs;
    this.keyGenerator = keyGenerator;
    this.schemaProvider = schemaProvider;
    this.instantTime = instantTime;
  }

  @Override
  public void writeRecord(SinkRecord record) throws IOException {
    AvroConvertor convertor = new AvroConvertor(HoodieSchema.fromAvroSchema(schemaProvider.getSourceSchema()));
    Option<GenericRecord> avroRecord;
    switch (connectConfigs.getKafkaValueConverter()) {
      case KAFKA_AVRO_CONVERTER:
        avroRecord = Option.of((GenericRecord) record.value());
        break;
      case KAFKA_STRING_CONVERTER:
        avroRecord = Option.of(convertor.fromJson((String) record.value()));
        break;
      case KAFKA_JSON_CONVERTER:
        throw new UnsupportedEncodingException("Currently JSON objects are not supported");
      default:
        throw new IOException("Unsupported Kafka Format type (" + connectConfigs.getKafkaValueConverter() + ")");
    }

    // Tag records with a file ID based on kafka partition and hudi partition.
    HoodieRecord<?> hoodieRecord = new HoodieAvroRecord<>(keyGenerator.getKey(avroRecord.get()), new HoodieAvroPayload(avroRecord));
    String fileId = KafkaConnectUtils.hashDigest(String.format("%s-%s", record.kafkaPartition(), hoodieRecord.getPartitionPath()));
    hoodieRecord.unseal();
    hoodieRecord.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId));
    hoodieRecord.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
    hoodieRecord.seal();
    writeHudiRecord(hoodieRecord);
  }

  @Override
  public List<WriteStatus> close() {
    return flushRecords();
  }

  protected abstract void writeHudiRecord(HoodieRecord<?> record);

  protected abstract List<WriteStatus> flushRecords();
}
