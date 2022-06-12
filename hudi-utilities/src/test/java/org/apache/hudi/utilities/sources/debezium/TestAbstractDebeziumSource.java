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
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

public abstract class TestAbstractDebeziumSource extends UtilitiesTestBase {

  private static final String TEST_TOPIC_NAME = "hoodie_test";

  private final HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);
  private KafkaTestUtils testUtils;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false);
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
    props.setProperty("hoodie.deltastreamer.source.kafka.value.deserializer.class", StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

    return props;
  }

  protected abstract String getIndexName();

  protected abstract String getSourceClass();

  protected abstract String getSchema();

  protected abstract GenericRecord generateMetaFields(GenericRecord record);

  protected abstract void validateMetaFields(Dataset<Row> records);

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testDebeziumEvents(Operation operation) throws Exception {

    String sourceClass = getSourceClass();

    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    TypedProperties props = createPropsForJsonSource();

    SchemaProvider schemaProvider = new MockSchemaRegistryProvider(props, jsc, this);
    SourceFormatAdapter debeziumSource = new SourceFormatAdapter(UtilHelpers.createSource(sourceClass, props, jsc, sparkSession, schemaProvider, metrics));

    testUtils.sendMessages(TEST_TOPIC_NAME, new String[] {generateDebeziumEvent(operation).toString()});

    InputBatch<Dataset<Row>> fetch = debeziumSource.fetchNewDataInRowFormat(Option.empty(), 10);
    assertEquals(1, fetch.getBatch().get().count());

    // Ensure the before fields are picked for DELETE CDC Events,
    // and after fields are picked for INSERT and UPDATE CDC Events.
    final String fieldPrefix = (operation.equals(Operation.DELETE)) ? "before_" : "after_";
    assertTrue(fetch.getBatch().get().select("type").collectAsList().stream()
        .allMatch(r -> r.getString(0).startsWith(fieldPrefix)));
    assertTrue(fetch.getBatch().get().select("type").collectAsList().stream()
        .allMatch(r -> r.getString(0).startsWith(fieldPrefix)));

    // Validate DB specific meta fields
    validateMetaFields(fetch.getBatch().get());
  }

  private GenericRecord generateDebeziumEvent(Operation op) {
    Schema schema = new Schema.Parser().parse(getSchema());
    String indexName = getIndexName().concat(".ghschema.gharchive.Value");
    GenericRecord rec = new GenericData.Record(schema);
    rec.put(DebeziumConstants.INCOMING_OP_FIELD, op.op);
    rec.put(DebeziumConstants.INCOMING_TS_MS_FIELD, 100L);

    // Before
    Schema.Field beforeField = schema.getField(DebeziumConstants.INCOMING_BEFORE_FIELD);
    Schema beforeSchema = beforeField.schema().getTypes().get(beforeField.schema().getIndexNamed(indexName));
    GenericRecord beforeRecord = new GenericData.Record(beforeSchema);

    beforeRecord.put("id", 1);
    beforeRecord.put("date", "1/1/2020");
    beforeRecord.put("type", "before_type");
    beforeRecord.put("payload", "before_payload");
    beforeRecord.put("timestamp", 1000L);
    rec.put(DebeziumConstants.INCOMING_BEFORE_FIELD, beforeRecord);

    // After
    Schema.Field afterField = schema.getField(DebeziumConstants.INCOMING_AFTER_FIELD);
    Schema afterSchema = afterField.schema().getTypes().get(afterField.schema().getIndexNamed(indexName));
    GenericRecord afterRecord = new GenericData.Record(afterSchema);

    afterRecord.put("id", 1);
    afterRecord.put("date", "1/1/2021");
    afterRecord.put("type", "after_type");
    afterRecord.put("payload", "after_payload");
    afterRecord.put("timestamp", 3000L);
    rec.put(DebeziumConstants.INCOMING_AFTER_FIELD, afterRecord);

    return generateMetaFields(rec);
  }

  private static class MockSchemaRegistryProvider extends SchemaRegistryProvider {

    private final String schema;

    public MockSchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc, TestAbstractDebeziumSource source) {
      super(props, jssc);
      schema = source.getSchema();
    }

    @Override
    public String fetchSchemaFromRegistry(String registryUrl) throws IOException {
      return schema;
    }
  }

  private static Stream<Arguments> testArguments() {
    return Stream.of(
        arguments(Operation.INSERT),
        arguments(Operation.UPDATE),
        arguments(Operation.DELETE)
    );
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
}
