/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.CatalogUtils;
import org.apache.hudi.utils.SchemaBuilder;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link HoodieTableFactory}.
 */
public class TestHoodieTableFactory {
  private static final String AVRO_SCHEMA_FILE_PATH = Objects.requireNonNull(Thread.currentThread()
      .getContextClassLoader().getResource("test_read_schema.avsc")).toString();
  private static final String INFERRED_SCHEMA = "{\"type\":\"record\","
      + "\"name\":\"t1_record\","
      + "\"namespace\":\"hoodie.t1\","
      + "\"fields\":["
      + "{\"name\":\"uuid\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},"
      + "{\"name\":\"partition\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() throws IOException {
    this.conf = new Configuration();
    this.conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    this.conf.set(FlinkOptions.TABLE_NAME, "t1");
    StreamerUtil.initTableIfNotExists(this.conf);
  }

  @Test
  void testRequiredOptions() {
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");

    // createDynamicTableSource doesn't call sanity check, will not throw exception
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext1));
    // miss pk and precombine key will throw exception when create sink
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext1));

    // append mode does not throw
    this.conf.set(FlinkOptions.OPERATION, "insert");
    final MockContext sourceContext11 = MockContext.getInstance(this.conf, schema1, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext11));
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext11));
    //miss the pre combine key will be ok
    HoodieTableSink tableSink11 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext11);
    assertThat(tableSink11.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is(FlinkOptions.NO_PRE_COMBINE));
    this.conf.set(FlinkOptions.OPERATION, FlinkOptions.OPERATION.defaultValue());

    // a non-exists precombine key will throw exception
    ResolvedSchema schema2 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .build();
    this.conf.set(FlinkOptions.PRECOMBINE_FIELDS, "non_exist_field");
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema2, "f2");
    // createDynamicTableSource doesn't call sanity check, will not throw exception
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext2));
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext2));
    this.conf.set(FlinkOptions.PRECOMBINE_FIELDS, FlinkOptions.PRECOMBINE_FIELDS.defaultValue());

    // given the pk but miss the pre combine key will be ok
    ResolvedSchema schema3 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema3, "f2");
    HoodieTableSource tableSource = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext3);
    HoodieTableSink tableSink = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext3);
    // the precombine field is overwritten
    assertThat(tableSink.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is(FlinkOptions.NO_PRE_COMBINE));
    // precombine field not specified, use the default payload clazz
    assertThat(tableSource.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is(FlinkOptions.PAYLOAD_CLASS_NAME.defaultValue()));
    assertThat(tableSink.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is(FlinkOptions.PAYLOAD_CLASS_NAME.defaultValue()));

    // append mode given the pk but miss the pre combine key will be ok
    this.conf.set(FlinkOptions.OPERATION, "insert");
    HoodieTableSink tableSink3 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext3);
    assertThat(tableSink3.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is(FlinkOptions.NO_PRE_COMBINE));
    this.conf.set(FlinkOptions.OPERATION, FlinkOptions.OPERATION.defaultValue());

    this.conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, DefaultHoodieRecordPayload.class.getName());
    final MockContext sourceContext4 = MockContext.getInstance(this.conf, schema3, "f2");

    // createDynamicTableSource doesn't call sanity check, will not throw exception
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext4));
    // given pk but miss the pre combine key with DefaultHoodieRecordPayload should throw
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext4));
    this.conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, FlinkOptions.PAYLOAD_CLASS_NAME.defaultValue());

    // given pk and pre combine key will be ok
    ResolvedSchema schema4 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext5 = MockContext.getInstance(this.conf, schema4, "f2");

    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext5));
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext5));
    // precombine field specified(default ts), use DefaultHoodieRecordPayload as payload clazz
    HoodieTableSource tableSource5 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext5);
    HoodieTableSink tableSink5 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext5);
    assertThat(tableSource5.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is(EventTimeAvroPayload.class.getName()));
    assertThat(tableSink5.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is(EventTimeAvroPayload.class.getName()));

    // given pk and set pre combine key to no_precombine will be ok
    ResolvedSchema schema5 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    this.conf.set(FlinkOptions.PRECOMBINE_FIELDS, FlinkOptions.NO_PRE_COMBINE);
    final MockContext sourceContext6 = MockContext.getInstance(this.conf, schema5, "f2");

    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext6));
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext6));
  }

  @Test
  void testIndexTypeCheck() {
    ResolvedSchema schema = SchemaBuilder.instance()
            .field("f0", DataTypes.INT().notNull())
            .field("f1", DataTypes.VARCHAR(20))
            .field("f2", DataTypes.TIMESTAMP(3))
            .field("ts", DataTypes.TIMESTAMP(3))
            .primaryKey("f0")
            .build();

    // Index type unset. The default value will be ok
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext1));

    // Invalid index type will throw exception
    this.conf.set(FlinkOptions.INDEX_TYPE, "BUCKET_AA");
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema, "f2");
    assertThrows(IllegalArgumentException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext2));

    // Valid index type will be ok
    this.conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext3));
  }

  @Test
  void testTableTypeCheck() {
    ResolvedSchema schema = SchemaBuilder.instance()
            .field("f0", DataTypes.INT().notNull())
            .field("f1", DataTypes.VARCHAR(20))
            .field("f2", DataTypes.TIMESTAMP(3))
            .field("ts", DataTypes.TIMESTAMP(3))
            .primaryKey("f0")
            .build();

    // Table type unset. The default value will be ok
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext1));

    // Invalid table type will throw exception if the hoodie.properties does not exist.
    this.conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath() + "_NOT_EXIST_TABLE_PATH");
    this.conf.set(FlinkOptions.TABLE_TYPE, "INVALID_TABLE_TYPE");
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema, "f2");
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext2));
    this.conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());

    // Invalid table type will be ok if the hoodie.properties exists.
    this.conf.set(FlinkOptions.TABLE_TYPE, "INVALID_TABLE_TYPE");
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext3));

    // Valid table type will be ok
    this.conf.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
    final MockContext sourceContext4 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext4));

    // Setup the table type correctly for hoodie.properties
    HoodieTableSink hoodieTableSink = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext4);
    assertThat(hoodieTableSink.getConf().get(FlinkOptions.TABLE_TYPE), is("COPY_ON_WRITE"));

    // Valid table type will be ok
    this.conf.set(FlinkOptions.TABLE_TYPE, "COPY_ON_WRITE");
    final MockContext sourceContext5 = MockContext.getInstance(this.conf, schema, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext5));
  }

  @Test
  void testSupplementTableConfig() throws Exception {
    String tablePath = new File(tempFile.getAbsolutePath(), "dummy").getAbsolutePath();
    // add pk and pre-combine key to table config
    Configuration tableConf = new Configuration();
    tableConf.set(FlinkOptions.PATH, tablePath);
    tableConf.set(FlinkOptions.TABLE_NAME, "t2");
    tableConf.set(FlinkOptions.RECORD_KEY_FIELD, "f0,f1");
    tableConf.set(FlinkOptions.PRECOMBINE_FIELDS, "f2");
    tableConf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    tableConf.set(FlinkOptions.PAYLOAD_CLASS_NAME, "my_payload");
    tableConf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    StreamerUtil.initTableIfNotExists(tableConf);

    Configuration writeConf = new Configuration();
    writeConf.set(FlinkOptions.PATH, tablePath);
    writeConf.set(FlinkOptions.TABLE_NAME, "t2");

    // fallback to table config
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(writeConf, schema1, "f2");
    HoodieTableSource source1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    HoodieTableSink sink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext1);
    assertThat("pk not provided, fallback to table config",
        source1.getConf().get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat("pk not provided, fallback to table config",
        sink1.getConf().get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat("pre-combine key not provided, fallback to table config",
        source1.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is("f2"));
    assertThat("pre-combine key not provided, fallback to table config",
        sink1.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is("f2"));
    assertThat("table type not provided, fallback to table config",
        source1.getConf().get(FlinkOptions.TABLE_TYPE), is(FlinkOptions.TABLE_TYPE_MERGE_ON_READ));
    assertThat("table type not provided, fallback to table config",
        sink1.getConf().get(FlinkOptions.TABLE_TYPE), is(FlinkOptions.TABLE_TYPE_MERGE_ON_READ));
    assertThat("payload class not provided, fallback to table config",
        source1.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is("my_payload"));
    assertThat("payload class not provided, fallback to table config",
        sink1.getConf().get(FlinkOptions.PAYLOAD_CLASS_NAME), is("my_payload"));

    // write config always has higher priority
    // set up a different primary key and pre_combine key with table config options
    writeConf.set(FlinkOptions.RECORD_KEY_FIELD, "f0");
    writeConf.set(FlinkOptions.PRECOMBINE_FIELDS, "f1");

    final MockContext sourceContext2 = MockContext.getInstance(writeConf, schema1, "f2");
    HoodieTableSource source2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    HoodieTableSink sink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sourceContext2);
    assertThat("choose pk from write config",
        source2.getConf().get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat("choose pk from write config",
        sink2.getConf().get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat("choose preCombine key from write config",
        source2.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is("f1"));
    assertThat("choose preCombine pk from write config",
        sink2.getConf().get(FlinkOptions.PRECOMBINE_FIELDS), is("f1"));

    writeConf.removeConfig(FlinkOptions.RECORD_KEY_FIELD);
    writeConf.removeConfig(FlinkOptions.PRECOMBINE_FIELDS);

    // pk defined in table config but missing in schema will throw
    ResolvedSchema schema2 = SchemaBuilder.instance()
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .build();
    final MockContext sourceContext3 = MockContext.getInstance(writeConf, schema2, "f2");
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext3),
        "createDynamicTableSource won't call sanity check");
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext3),
        "f0 is in table config as record key, but missing in input schema");
  }

  @Test
  void testInferAvroSchemaForSource() {
    // infer the schema if not specified
    final HoodieTableSource tableSource1 =
        (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(MockContext.getInstance(this.conf));
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.SOURCE_AVRO_SCHEMA), is(INFERRED_SCHEMA));

    // set up the explicit schema using the file path
    this.conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, AVRO_SCHEMA_FILE_PATH);
    HoodieTableSource tableSource2 =
        (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(MockContext.getInstance(this.conf));
    Configuration conf2 = tableSource2.getConf();
    assertNull(conf2.get(FlinkOptions.SOURCE_AVRO_SCHEMA), "expect schema string as null");

    // infer special avro data types that needs namespace
    this.conf.removeConfig(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    ResolvedSchema schema3 = SchemaBuilder.instance()
        .field("f_decimal", DataTypes.DECIMAL(3, 2).notNull())
        .field("f_map", DataTypes.MAP(DataTypes.VARCHAR(20), DataTypes.VARCHAR(10)))
        .field("f_array", DataTypes.ARRAY(DataTypes.VARCHAR(10)))
        .field("f_record", DataTypes.ROW(DataTypes.FIELD("r1", DataTypes.VARCHAR(10)), DataTypes.FIELD("r2", DataTypes.INT())))
        .primaryKey("f_decimal")
        .build();
    final HoodieTableSink tableSink3 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf, schema3, ""));
    final Configuration conf3 = tableSink3.getConf();
    final String expected = AvroSchemaConverter.convertToSchema(schema3.toSourceRowDataType().getLogicalType(), AvroSchemaUtils.getAvroRecordQualifiedName("t1")).toString();
    assertThat(conf3.get(FlinkOptions.SOURCE_AVRO_SCHEMA), is(expected));
  }

  @Test
  void testSetupHoodieKeyOptionsForSource() {
    this.conf.set(FlinkOptions.RECORD_KEY_FIELD, "dummyField");
    this.conf.set(FlinkOptions.KEYGEN_CLASS_NAME, "dummyKeyGenClass");
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.BIGINT())
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat(conf1.get(FlinkOptions.KEYGEN_CLASS_NAME), is("dummyKeyGenClass"));

    // definition with complex primary keys and partition paths
    this.conf.removeConfig(FlinkOptions.KEYGEN_CLASS_NAME);
    ResolvedSchema schema2 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20).notNull())
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0", "f1")
        .build();
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema2, "f2");
    final HoodieTableSource tableSource2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    final Configuration conf2 = tableSource2.getConf();
    assertThat(conf2.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf2.get(FlinkOptions.KEYGEN_CLASS_NAME), is(ComplexAvroKeyGenerator.class.getName()));

    // definition with complex primary keys and empty partition paths
    this.conf.removeConfig(FlinkOptions.KEYGEN_CLASS_NAME);
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema2, "");
    final HoodieTableSource tableSource3 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext3);
    final Configuration conf3 = tableSource3.getConf();
    assertThat(conf3.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf3.get(FlinkOptions.KEYGEN_CLASS_NAME), is(NonpartitionedAvroKeyGenerator.class.getName()));
  }

  @Test
  void testSetupHiveOptionsForSource() {
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();

    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_DB), is("db1"));
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_TABLE), is("t1"));
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME), is(MultiPartKeysValueExtractor.class.getName()));

    // set up hive style partitioning is true.
    this.conf.set(FlinkOptions.HIVE_SYNC_DB, "db2");
    this.conf.set(FlinkOptions.HIVE_SYNC_TABLE, "t2");
    this.conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);

    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    final Configuration conf2 = tableSource2.getConf();
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_DB), is("db2"));
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_TABLE), is("t2"));
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME), is(MultiPartKeysValueExtractor.class.getName()));
  }

  @Test
  void testSetupCleaningOptionsForSource() {
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    // set up new retains commits that is less than min archive commits
    this.conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 11);

    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.ARCHIVE_MIN_COMMITS), is(FlinkOptions.ARCHIVE_MIN_COMMITS.defaultValue()));
    assertThat(conf1.get(FlinkOptions.ARCHIVE_MAX_COMMITS), is(FlinkOptions.ARCHIVE_MAX_COMMITS.defaultValue()));

    // set up new retains commits that is greater than min archive commits
    final int retainCommits = FlinkOptions.ARCHIVE_MIN_COMMITS.defaultValue() + 5;
    this.conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, retainCommits);

    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    final Configuration conf2 = tableSource2.getConf();
    assertThat(conf2.get(FlinkOptions.ARCHIVE_MIN_COMMITS), is(retainCommits + 10));
    assertThat(conf2.get(FlinkOptions.ARCHIVE_MAX_COMMITS), is(retainCommits + 20));
  }

  @Test
  void testSetupReadOptionsForSource() {
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    // set up new retains commits that is less than min archive commits
    this.conf.set(FlinkOptions.READ_END_COMMIT, "123");

    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.QUERY_TYPE), is(FlinkOptions.QUERY_TYPE_INCREMENTAL));

    this.conf.removeConfig(FlinkOptions.READ_END_COMMIT);
    this.conf.set(FlinkOptions.READ_START_COMMIT, "123");
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    final Configuration conf2 = tableSource2.getConf();
    assertThat(conf2.get(FlinkOptions.QUERY_TYPE), is(FlinkOptions.QUERY_TYPE_INCREMENTAL));
  }

  @Test
  void testBucketIndexOptionForSink() {
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20).notNull())
        .field("f2", DataTypes.TIMESTAMP(3))
        .primaryKey("f0", "f1")
        .build();

    this.conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());

    // default use recordKey fields
    final MockContext context = MockContext.getInstance(this.conf, schema1, "f2");
    HoodieTableSink tableSink = (HoodieTableSink) (new HoodieTableFactory().createDynamicTableSink(context));
    final Configuration conf = tableSink.getConf();
    assertThat(conf.get(FlinkOptions.INDEX_KEY_FIELD), is("f0,f1"));

    this.conf.set(FlinkOptions.INDEX_KEY_FIELD, "f0");
    final MockContext context2 = MockContext.getInstance(this.conf, schema1, "f2");
    HoodieTableSink tableSink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(context2);
    final Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.get(FlinkOptions.INDEX_KEY_FIELD), is("f0"));

    this.conf.set(FlinkOptions.INDEX_KEY_FIELD, "f1");
    final MockContext context3 = MockContext.getInstance(this.conf, schema1, "f2");
    HoodieTableSink tableSink3 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(context3);
    final Configuration conf3 = tableSink3.getConf();
    assertThat(conf3.get(FlinkOptions.INDEX_KEY_FIELD), is("f1"));

    this.conf.set(FlinkOptions.INDEX_KEY_FIELD, "f0,f1");
    final MockContext context4 = MockContext.getInstance(this.conf, schema1, "f2");
    HoodieTableSink tableSink4 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(context4);
    final Configuration conf4 = tableSink4.getConf();
    assertThat(conf4.get(FlinkOptions.INDEX_KEY_FIELD), is("f0,f1"));

    // index key field is not a subset of or equal to the recordKey fields, will throw exception
    this.conf.set(FlinkOptions.INDEX_KEY_FIELD, "f2");
    final MockContext context5 = MockContext.getInstance(this.conf, schema1, "f2");
    assertThrows(HoodieValidationException.class, () -> new HoodieTableFactory().createDynamicTableSource(context5));
  }

  @Test
  void testInferAvroSchemaForSink() {
    // infer the schema if not specified
    final HoodieTableSink tableSink1 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.SOURCE_AVRO_SCHEMA), is(INFERRED_SCHEMA));

    // set up the explicit schema using the file path
    this.conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, AVRO_SCHEMA_FILE_PATH);
    HoodieTableSink tableSink2 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    Configuration conf2 = tableSink2.getConf();
    assertNull(conf2.get(FlinkOptions.SOURCE_AVRO_SCHEMA), "expect schema string as null");

    // infer special avro data types that needs namespace
    this.conf.removeConfig(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH);
    ResolvedSchema schema3 = SchemaBuilder.instance()
        .field("f_decimal", DataTypes.DECIMAL(3, 2).notNull())
        .field("f_map", DataTypes.MAP(DataTypes.VARCHAR(20), DataTypes.VARCHAR(10)))
        .field("f_array", DataTypes.ARRAY(DataTypes.VARCHAR(10)))
        .field("f_record", DataTypes.ROW(DataTypes.FIELD("r1", DataTypes.VARCHAR(10)), DataTypes.FIELD("r2", DataTypes.INT())))
        .primaryKey("f_decimal")
        .build();
    final HoodieTableSink tableSink3 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf, schema3, ""));
    final Configuration conf3 = tableSink3.getConf();
    final String expected = AvroSchemaConverter.convertToSchema(schema3.toSinkRowDataType().getLogicalType(), AvroSchemaUtils.getAvroRecordQualifiedName("t1")).toString();
    assertThat(conf3.get(FlinkOptions.SOURCE_AVRO_SCHEMA), is(expected));
  }

  @Test
  void testSetupHoodieKeyOptionsForSink() {
    this.conf.set(FlinkOptions.RECORD_KEY_FIELD, "dummyField");
    this.conf.set(FlinkOptions.KEYGEN_CLASS_NAME, "dummyKeyGenClass");
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.BIGINT())
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sinkContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext1);
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat(conf1.get(FlinkOptions.KEYGEN_CLASS_NAME), is("dummyKeyGenClass"));

    // definition with complex primary keys and partition paths
    this.conf.removeConfig(FlinkOptions.KEYGEN_CLASS_NAME);
    ResolvedSchema schema2 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20).notNull())
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0", "f1")
        .build();
    final MockContext sinkContext2 = MockContext.getInstance(this.conf, schema2, "f2");
    final HoodieTableSink tableSink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext2);
    final Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf2.get(FlinkOptions.KEYGEN_CLASS_NAME), is(ComplexAvroKeyGenerator.class.getName()));

    // definition with complex primary keys and empty partition paths
    this.conf.removeConfig(FlinkOptions.KEYGEN_CLASS_NAME);
    final MockContext sinkContext3 = MockContext.getInstance(this.conf, schema2, "");
    final HoodieTableSink tableSink3 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext3);
    final Configuration conf3 = tableSink3.getConf();
    assertThat(conf3.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf3.get(FlinkOptions.KEYGEN_CLASS_NAME), is(NonpartitionedAvroKeyGenerator.class.getName()));

    // definition of bucket index
    this.conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    final MockContext sinkContext4 = MockContext.getInstance(this.conf, schema2, "");
    final HoodieTableSink tableSink4 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext4);
    final Configuration conf4 = tableSink4.getConf();
    assertThat(conf4.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf4.get(FlinkOptions.INDEX_KEY_FIELD), is("f0,f1"));
    assertThat(conf4.get(FlinkOptions.INDEX_TYPE), is(HoodieIndex.IndexType.BUCKET.name()));
    assertThat(conf4.get(FlinkOptions.KEYGEN_CLASS_NAME), is(NonpartitionedAvroKeyGenerator.class.getName()));
  }

  @Test
  void testSetupHiveOptionsForSink() {
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();

    final MockContext sinkContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext1);
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_DB), is("db1"));
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_TABLE), is("t1"));
    assertThat(conf1.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME), is(MultiPartKeysValueExtractor.class.getName()));

    // set up hive style partitioning is true.
    this.conf.set(FlinkOptions.HIVE_SYNC_DB, "db2");
    this.conf.set(FlinkOptions.HIVE_SYNC_TABLE, "t2");
    this.conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);

    final MockContext sinkContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext2);
    final Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_DB), is("db2"));
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_TABLE), is("t2"));
    assertThat(conf2.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME), is(MultiPartKeysValueExtractor.class.getName()));
  }

  @Test
  void testSetupCleaningOptionsForSink() {
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    // set up new retains commits that is less than min archive commits
    this.conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 11);

    final MockContext sinkContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext1);
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.ARCHIVE_MIN_COMMITS), is(FlinkOptions.ARCHIVE_MIN_COMMITS.defaultValue()));
    assertThat(conf1.get(FlinkOptions.ARCHIVE_MAX_COMMITS), is(FlinkOptions.ARCHIVE_MAX_COMMITS.defaultValue()));

    // set up new retains commits that is greater than min archive commits
    final int retainCommits = FlinkOptions.ARCHIVE_MIN_COMMITS.defaultValue() + 5;
    this.conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, retainCommits);

    final MockContext sinkContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext2);
    final Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.get(FlinkOptions.ARCHIVE_MIN_COMMITS), is(retainCommits + 10));
    assertThat(conf2.get(FlinkOptions.ARCHIVE_MAX_COMMITS), is(retainCommits + 20));
  }

  @Test
  void testSetupTimestampBasedKeyGenForSink() {
    this.conf.set(FlinkOptions.RECORD_KEY_FIELD, "dummyField");
    // definition with simple primary key and partition path
    ResolvedSchema schema1 = SchemaBuilder.instance()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "ts");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat(conf1.get(FlinkOptions.KEYGEN_CLASS_NAME), is(TimestampBasedAvroKeyGenerator.class.getName()));
    assertThat(conf1.getString(TIMESTAMP_TYPE_FIELD.key(), "dummy"),
        is("EPOCHMILLISECONDS"));
    assertThat(conf1.getString(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), "dummy"),
        is(FlinkOptions.PARTITION_FORMAT_HOUR));
    assertThat(conf1.getString(TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key(), "dummy"),
        is("UTC"));
  }

  @Test
  void testSetupWriteOptionsForSink() {
    final HoodieTableSink tableSink1 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.PRE_COMBINE), is(true));
    // check setup database name and table name automatically
    assertThat(conf1.get(FlinkOptions.TABLE_NAME), is("t1"));
    assertThat(conf1.get(FlinkOptions.DATABASE_NAME), is("db1"));

    // set up operation as 'insert'
    this.conf.set(FlinkOptions.OPERATION, "insert");
    HoodieTableSink tableSink2 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.get(FlinkOptions.PRE_COMBINE), is(false));
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Mock dynamic table factory context.
   */
  private static class MockContext implements DynamicTableFactory.Context {
    private final Configuration conf;
    private final ResolvedSchema schema;
    private final List<String> partitions;

    private MockContext(Configuration conf, ResolvedSchema schema, List<String> partitions) {
      this.conf = conf;
      this.schema = schema;
      this.partitions = partitions;
    }

    static MockContext getInstance(Configuration conf) {
      return getInstance(conf, TestConfigurations.TABLE_SCHEMA, Collections.singletonList("partition"));
    }

    static MockContext getInstance(Configuration conf, ResolvedSchema schema, String partition) {
      return getInstance(conf, schema, Collections.singletonList(partition));
    }

    static MockContext getInstance(Configuration conf, ResolvedSchema schema, List<String> partitions) {
      return new MockContext(conf, schema, partitions);
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
      return ObjectIdentifier.of("hudi", "db1", "t1");
    }

    @Override
    public ResolvedCatalogTable getCatalogTable() {
      CatalogTable catalogTable = CatalogUtils.createCatalogTable(Schema.newBuilder().fromResolvedSchema(schema).build(),
          partitions, conf.toMap(), "mock source table");
      return new ResolvedCatalogTable(catalogTable, schema);
    }

    @Override
    public ReadableConfig getConfiguration() {
      return conf;
    }

    @Override
    public ClassLoader getClassLoader() {
      return null;
    }

    @Override
    public boolean isTemporary() {
      return false;
    }
  }
}
