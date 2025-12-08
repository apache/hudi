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

package org.apache.hudi.writers;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.writers.AbstractConnectWriter;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.schema.SchemaProvider;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAbstractConnectWriter {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int PARTITION_NUMBER = 4;
  private static final int NUM_RECORDS = 10;
  private static final int RECORD_KEY_INDEX = 0;

  private KafkaConnectConfigs configs;
  private TestKeyGenerator keyGenerator;
  private SchemaProvider schemaProvider;
  private long currentKafkaOffset;

  @BeforeEach
  public void setUp() throws Exception {
    keyGenerator = new TestKeyGenerator(new TypedProperties());
    schemaProvider = new TestSchemaProvider();
  }

  @ParameterizedTest
  @EnumSource(value = TestInputFormats.class)
  public void testAbstractWriterForAllFormats(TestInputFormats inputFormats) throws Exception {
    HoodieSchema schema = schemaProvider.getSourceHoodieSchema();
    List<?> inputRecords;
    List<HoodieRecord> expectedRecords;

    String formatConverter;
    switch (inputFormats) {
      case JSON_STRING:
        formatConverter = AbstractConnectWriter.KAFKA_STRING_CONVERTER;
        GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema.toAvroSchema(), schema.toAvroSchema());
        inputRecords = SchemaTestUtil.generateTestJsonRecords(0, NUM_RECORDS);
        expectedRecords = ((List<String>) inputRecords).stream().map(s -> {
          try {
            return HoodieAvroUtils.rewriteRecord((GenericRecord) reader.read(null, DecoderFactory.get().jsonDecoder(schema.getAvroSchema(), s)),
                schema.getAvroSchema());
          } catch (IOException exception) {
            throw new HoodieException("Error converting JSON records to AVRO");
          }
        }).map(p -> convertToHoodieRecords(p, p.get(RECORD_KEY_INDEX).toString(), "000/00/00")).collect(Collectors.toList());
        break;
      case AVRO:
        formatConverter = AbstractConnectWriter.KAFKA_AVRO_CONVERTER;
        inputRecords = SchemaTestUtil.generateTestRecords(0, NUM_RECORDS);
        expectedRecords = inputRecords.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s, schema.toAvroSchema()))
            .map(p -> convertToHoodieRecords(p, p.get(RECORD_KEY_INDEX).toString(), "000/00/00")).collect(Collectors.toList());
        break;
      default:
        throw new HoodieException("Unknown test scenario " + inputFormats);
    }

    configs = KafkaConnectConfigs.newBuilder()
        .withProperties(
            Collections.singletonMap(KafkaConnectConfigs.KAFKA_VALUE_CONVERTER, formatConverter))
        .build();
    AbstractHudiConnectWriterTestWrapper writer = new AbstractHudiConnectWriterTestWrapper(
        configs,
        keyGenerator,
        schemaProvider);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writer.writeRecord(getNextKafkaRecord(inputRecords.get(i)));
    }

    validateRecords(writer.getWrittenRecords(), expectedRecords);
  }

  private static void validateRecords(List<HoodieRecord> actualRecords, List<HoodieRecord> expectedRecords) {
    assertEquals(actualRecords.size(), expectedRecords.size());

    actualRecords.sort(Comparator.comparing(HoodieRecord::getRecordKey));
    expectedRecords.sort(Comparator.comparing(HoodieRecord::getRecordKey));

    // iterate through the elements and compare them one by one using
    // the provided comparator.
    Iterator<HoodieRecord> it1 = actualRecords.iterator();
    Iterator<HoodieRecord> it2 = expectedRecords.iterator();
    while (it1.hasNext()) {
      HoodieRecord t1 = it1.next();
      HoodieRecord t2 = it2.next();
      assertEquals(t1.getRecordKey(), t2.getRecordKey());
    }
  }

  private SinkRecord getNextKafkaRecord(Object record) {
    return new SinkRecord(TOPIC_NAME, PARTITION_NUMBER,
        org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA,
        getUTF8Bytes("key-" + currentKafkaOffset),
        org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA,
        record, currentKafkaOffset++);
  }

  private static class AbstractHudiConnectWriterTestWrapper extends AbstractConnectWriter {

    private List<HoodieRecord> writtenRecords;

    public AbstractHudiConnectWriterTestWrapper(KafkaConnectConfigs connectConfigs, KeyGenerator keyGenerator, SchemaProvider schemaProvider) {
      super(connectConfigs, keyGenerator, schemaProvider, "000");
      writtenRecords = new ArrayList<>();
    }

    public List<HoodieRecord> getWrittenRecords() {
      return writtenRecords;
    }

    @Override
    protected void writeHudiRecord(HoodieRecord<?> record) {
      writtenRecords.add(record);
    }

    @Override
    protected List<WriteStatus> flushRecords() {
      return null;
    }
  }

  private static HoodieRecord convertToHoodieRecords(IndexedRecord iRecord, String key, String partitionPath) {
    return new HoodieAvroRecord<>(new HoodieKey(key, partitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) iRecord)));
  }

  private enum TestInputFormats {
    AVRO,
    JSON_STRING
  }

  static class TestKeyGenerator extends KeyGenerator {

    protected TestKeyGenerator(TypedProperties config) {
      super(config);
    }

    @Override
    public HoodieKey getKey(GenericRecord record) {
      return new HoodieKey(record.get(RECORD_KEY_INDEX).toString(), "000/00/00");
    }
  }

  static class TestSchemaProvider extends SchemaProvider {

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      try {
        return SchemaTestUtil.getSimpleSchema();
      } catch (IOException exception) {
        throw new HoodieException("Fatal error parsing schema", exception);
      }
    }
  }
}
