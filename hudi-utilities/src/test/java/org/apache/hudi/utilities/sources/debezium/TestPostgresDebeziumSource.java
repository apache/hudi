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

package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestPostgresDebeziumSource extends UtilitiesTestBase {

  private static final Logger LOG = LogManager.getLogger(TestPostgresDebeziumSource.class);
  private static String TEST_TOPIC_NAME = "hoodie_test";
  private static final String POSTGRES_GITHUB_SCHEMA = "{\n" +
      "  \"connect.name\": \"postgres.ghschema.gharchive.Envelope\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"default\": null,\n" +
      "      \"name\": \"before\",\n" +
      "      \"type\": [\n" +
      "        \"null\",\n" +
      "        {\n" +
      "          \"connect.name\": \"postgres.ghschema.gharchive.Value\",\n" +
      "          \"fields\": [\n" +
      "            {\n" +
      "              \"name\": \"id\",\n" +
      "              \"type\": \"string\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"name\": \"date\",\n" +
      "              \"type\": \"string\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"timestamp\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"long\"\n" +
      "              ]\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"type\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"string\"\n" +
      "              ]\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"payload\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"string\"\n" +
      "              ]\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"org\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"string\"\n" +
      "              ]\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"created_at\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"long\"\n" +
      "              ]\n" +
      "            },\n" +
      "            {\n" +
      "              \"default\": null,\n" +
      "              \"name\": \"public\",\n" +
      "              \"type\": [\n" +
      "                \"null\",\n" +
      "                \"boolean\"\n" +
      "              ]\n" +
      "            }\n" +
      "          ],\n" +
      "          \"name\": \"Value\",\n" +
      "          \"type\": \"record\"\n" +
      "        }\n" +
      "      ]\n" +
      "    },\n" +
      "    {\n" +
      "      \"default\": null,\n" +
      "      \"name\": \"after\",\n" +
      "      \"type\": [\n" +
      "        \"null\",\n" +
      "        \"Value\"\n" +
      "      ]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"source\",\n" +
      "      \"type\": {\n" +
      "        \"connect.name\": \"io.debezium.connector.postgresql.Source\",\n" +
      "        \"fields\": [\n" +
      "          {\n" +
      "            \"name\": \"connector\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"name\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"ts_ms\",\n" +
      "            \"type\": \"long\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"db\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"default\": null,\n" +
      "            \"name\": \"sequence\",\n" +
      "            \"type\": [\n" +
      "              \"null\",\n" +
      "              \"string\"\n" +
      "            ]\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"schema\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"table\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"default\": null,\n" +
      "            \"name\": \"txId\",\n" +
      "            \"type\": [\n" +
      "              \"null\",\n" +
      "              \"long\"\n" +
      "            ]\n" +
      "          },\n" +
      "          {\n" +
      "            \"default\": null,\n" +
      "            \"name\": \"lsn\",\n" +
      "            \"type\": [\n" +
      "              \"null\",\n" +
      "              \"long\"\n" +
      "            ]\n" +
      "          },\n" +
      "          {\n" +
      "            \"default\": null,\n" +
      "            \"name\": \"xmin\",\n" +
      "            \"type\": [\n" +
      "              \"null\",\n" +
      "              \"long\"\n" +
      "            ]\n" +
      "          }\n" +
      "        ],\n" +
      "        \"name\": \"Source\",\n" +
      "        \"namespace\": \"io.debezium.connector.postgresql\",\n" +
      "        \"type\": \"record\"\n" +
      "      }\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"op\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"default\": null,\n" +
      "      \"name\": \"ts_ms\",\n" +
      "      \"type\": [\n" +
      "        \"null\",\n" +
      "        \"long\"\n" +
      "      ]\n" +
      "    },\n" +
      "    {\n" +
      "      \"default\": null,\n" +
      "      \"name\": \"transaction\",\n" +
      "      \"type\": [\n" +
      "        \"null\",\n" +
      "        {\n" +
      "          \"fields\": [\n" +
      "            {\n" +
      "              \"name\": \"id\",\n" +
      "              \"type\": \"string\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"name\": \"total_order\",\n" +
      "              \"type\": \"long\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"name\": \"data_collection_order\",\n" +
      "              \"type\": \"long\"\n" +
      "            }\n" +
      "          ],\n" +
      "          \"name\": \"ConnectDefault\",\n" +
      "          \"namespace\": \"io.confluent.connect.avro\",\n" +
      "          \"type\": \"record\"\n" +
      "        }\n" +
      "      ]\n" +
      "    }\n" +
      "  ],\n" +
      "  \"name\": \"Envelope\",\n" +
      "  \"namespace\": \"postgres.ghschema.gharchive\",\n" +
      "  \"type\": \"record\"\n" +
      "}";
  private static final Schema POSTGRES_GITHUB_AVRO_SCHEMA = new Schema.Parser().parse(POSTGRES_GITHUB_SCHEMA);

  private MockSchemaRegistryProvider schemaProvider;
  private KafkaTestUtils testUtils;
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass(false);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }


  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    testUtils = new KafkaTestUtils();
    testUtils.setup();
    schemaProvider = new MockSchemaRegistryProvider(createPropsForJsonSource(), jsc);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    testUtils.teardown();
  }

  private TypedProperties createPropsForJsonSource() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", "earliest");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.deltastreamer.schemaprovider.registry.url", "localhost");
    props.setProperty("schema.registry.url", "localhost");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

    return props;
  }

  @Test
  public void testJsonKafkaSource() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    TypedProperties props = createPropsForJsonSource();

    PostgresDebeziumSource postgresDebeziumSource = new PostgresDebeziumSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter debeziumSource = new SourceFormatAdapter(postgresDebeziumSource);
    TypedProperties prop1 = new TypedProperties();
    prop1.put("key.serializer", StringSerializer.class);
    prop1.put("value.serializer", StringSerializer.class);
    prop1.put("bootstrap.servers", testUtils.brokerAddress());
    //prop1.setProperty("schema.registry.url", "localhost");

    //Producer<String, String> mockProducer = new KafkaProducer<>(prop1);
    //ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(TEST_TOPIC_NAME, "key", "vale");// generateDebeziumEvent(Operation.INSERT));
    //mockProducer.send(kafkaRecord);
    LOG.error("WNI " + testUtils.brokerAddress());
    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    //assertEquals(Option.empty(), debeziumSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE).getBatch());

    //Map<String, GenericRecord> testMap = new HashMap<>();
    //testMap.put("key", generateDebeziumEvent(Operation.INSERT));
    testUtils.sendMessages(TEST_TOPIC_NAME, new String[] { generateDebeziumEvent(Operation.INSERT).toString() });

    InputBatch<Dataset<Row>> fetch1 = debeziumSource.fetchNewDataInRowFormat(Option.empty(), 900);
    assertEquals(1, fetch1.getBatch().get().count());
  }

  private static GenericRecord generateDebeziumEvent(Operation op) {
    GenericRecord rec = new GenericData.Record(POSTGRES_GITHUB_AVRO_SCHEMA);
    rec.put(DebeziumConstants.INCOMING_OP_FIELD, op.op);
    rec.put(DebeziumConstants.INCOMING_TS_MS_FIELD, 100L);

    // Before
    Schema.Field beforeField = POSTGRES_GITHUB_AVRO_SCHEMA.getField(DebeziumConstants.INCOMING_BEFORE_FIELD);
    Schema beforeSchema = beforeField.schema().getTypes().get(beforeField.schema().getIndexNamed("postgres.ghschema.gharchive.Value"));
    GenericRecord beforeRecord = new GenericData.Record(beforeSchema);

    beforeRecord.put("id", 1);
    beforeRecord.put("date", "1/1/2020");
    beforeRecord.put("timestamp", 1000L);
    rec.put(DebeziumConstants.INCOMING_BEFORE_FIELD, beforeRecord);

    // After
    Schema.Field afterField = POSTGRES_GITHUB_AVRO_SCHEMA.getField(DebeziumConstants.INCOMING_AFTER_FIELD);
    Schema afterSchema = afterField.schema().getTypes().get(afterField.schema().getIndexNamed("postgres.ghschema.gharchive.Value"));
    GenericRecord afterRecord = new GenericData.Record(afterSchema);

    afterRecord.put("id", 1);
    afterRecord.put("date", "1/1/2021");
    afterRecord.put("timestamp", 3000L);
    rec.put(DebeziumConstants.INCOMING_AFTER_FIELD, afterRecord);

    // Source
    GenericRecord sourceRecord = new GenericData.Record(POSTGRES_GITHUB_AVRO_SCHEMA.getField(DebeziumConstants.INCOMING_SOURCE_FIELD).schema());
    sourceRecord.put("name", "postgres");
    sourceRecord.put("connector", "test");
    sourceRecord.put("db", "postgres");
    sourceRecord.put("schema", "ghschema");
    sourceRecord.put("table", "gharchive");
    sourceRecord.put("ts_ms", 3000L);
    sourceRecord.put("txId", 100L);
    sourceRecord.put("lsn", 1L);
    sourceRecord.put("xmin", 111L);
    rec.put(DebeziumConstants.INCOMING_SOURCE_FIELD, sourceRecord);

    return rec;
  }

  private enum Operation {
    INSERT("c"),
    UPDATE("u"),
    DELETE("d");

    public final String op;

    Operation(String op) {
      this.op = op;
    }
  }

  private static class MockSchemaRegistryProvider extends SchemaRegistryProvider {

    public MockSchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public String fetchSchemaFromRegistry(String registryUrl) throws IOException {
      return POSTGRES_GITHUB_SCHEMA;
    }
  }
}
