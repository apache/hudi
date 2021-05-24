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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
      + "\"name\":\"record\","
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
    this.conf.setString(FlinkOptions.PATH, tempFile.getAbsolutePath());
    this.conf.setString(FlinkOptions.TABLE_NAME, "t1");
    StreamerUtil.initTableIfNotExists(this.conf);
  }

  @Test
  void testRequiredOptionsForSource() {
    // miss pk and pre combine key will throw exception
    TableSchema schema1 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    assertThrows(ValidationException.class, () -> new HoodieTableFactory().createDynamicTableSource(sourceContext1));
    assertThrows(ValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext1));

    // given the pk and miss the pre combine key will throw exception
    TableSchema schema2 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema2, "f2");
    assertThrows(ValidationException.class, () -> new HoodieTableFactory().createDynamicTableSource(sourceContext2));
    assertThrows(ValidationException.class, () -> new HoodieTableFactory().createDynamicTableSink(sourceContext2));

    // given pk and pre combine key will be ok
    TableSchema schema3 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema3, "f2");

    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSource(sourceContext3));
    assertDoesNotThrow(() -> new HoodieTableFactory().createDynamicTableSink(sourceContext3));
  }

  @Test
  void testInferAvroSchemaForSource() {
    // infer the schema if not specified
    final HoodieTableSource tableSource1 =
        (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(MockContext.getInstance(this.conf));
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.READ_AVRO_SCHEMA), is(INFERRED_SCHEMA));

    // set up the explicit schema using the file path
    this.conf.setString(FlinkOptions.READ_AVRO_SCHEMA_PATH, AVRO_SCHEMA_FILE_PATH);
    HoodieTableSource tableSource2 =
        (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(MockContext.getInstance(this.conf));
    Configuration conf2 = tableSource2.getConf();
    assertNull(conf2.get(FlinkOptions.READ_AVRO_SCHEMA), "expect schema string as null");
  }

  @Test
  void testSetupHoodieKeyOptionsForSource() {
    this.conf.setString(FlinkOptions.RECORD_KEY_FIELD, "dummyField");
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, "dummyKeyGenClass");
    // definition with simple primary key and partition path
    TableSchema schema1 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat(conf1.get(FlinkOptions.KEYGEN_CLASS), is("dummyKeyGenClass"));

    // definition with complex primary keys and partition paths
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, FlinkOptions.KEYGEN_CLASS.defaultValue());
    TableSchema schema2 = TableSchema.builder()
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
    assertThat(conf2.get(FlinkOptions.KEYGEN_CLASS), is(ComplexAvroKeyGenerator.class.getName()));

    // definition with complex primary keys and empty partition paths
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, FlinkOptions.KEYGEN_CLASS.defaultValue());
    final MockContext sourceContext3 = MockContext.getInstance(this.conf, schema2, "");
    final HoodieTableSource tableSource3 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext3);
    final Configuration conf3 = tableSource3.getConf();
    assertThat(conf3.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf3.get(FlinkOptions.KEYGEN_CLASS), is(NonpartitionedAvroKeyGenerator.class.getName()));
  }

  @Test
  void testSetupCleaningOptionsForSource() {
    // definition with simple primary key and partition path
    TableSchema schema1 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    // set up new retains commits that is less than min archive commits
    this.conf.setString(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "11");

    final MockContext sourceContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource1 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext1);
    final Configuration conf1 = tableSource1.getConf();
    assertThat(conf1.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), is(20));
    assertThat(conf1.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS), is(30));

    // set up new retains commits that is greater than min archive commits
    this.conf.setString(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "25");

    final MockContext sourceContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSource tableSource2 = (HoodieTableSource) new HoodieTableFactory().createDynamicTableSource(sourceContext2);
    final Configuration conf2 = tableSource2.getConf();
    assertThat(conf2.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), is(35));
    assertThat(conf2.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS), is(45));
  }

  @Test
  void testInferAvroSchemaForSink() {
    // infer the schema if not specified
    final HoodieTableSink tableSink1 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.READ_AVRO_SCHEMA), is(INFERRED_SCHEMA));

    // set up the explicit schema using the file path
    this.conf.setString(FlinkOptions.READ_AVRO_SCHEMA_PATH, AVRO_SCHEMA_FILE_PATH);
    HoodieTableSink tableSink2 =
        (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(MockContext.getInstance(this.conf));
    Configuration conf2 = tableSink2.getConf();
    assertNull(conf2.get(FlinkOptions.READ_AVRO_SCHEMA), "expect schema string as null");
  }

  @Test
  void testSetupHoodieKeyOptionsForSink() {
    this.conf.setString(FlinkOptions.RECORD_KEY_FIELD, "dummyField");
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, "dummyKeyGenClass");
    // definition with simple primary key and partition path
    TableSchema schema1 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    final MockContext sinkContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext1);
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.get(FlinkOptions.RECORD_KEY_FIELD), is("f0"));
    assertThat(conf1.get(FlinkOptions.KEYGEN_CLASS), is("dummyKeyGenClass"));

    // definition with complex primary keys and partition paths
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, FlinkOptions.KEYGEN_CLASS.defaultValue());
    TableSchema schema2 = TableSchema.builder()
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
    assertThat(conf2.get(FlinkOptions.KEYGEN_CLASS), is(ComplexAvroKeyGenerator.class.getName()));

    // definition with complex primary keys and empty partition paths
    this.conf.setString(FlinkOptions.KEYGEN_CLASS, FlinkOptions.KEYGEN_CLASS.defaultValue());
    final MockContext sinkContext3 = MockContext.getInstance(this.conf, schema2, "");
    final HoodieTableSink tableSink3 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext3);
    final Configuration conf3 = tableSink3.getConf();
    assertThat(conf3.get(FlinkOptions.RECORD_KEY_FIELD), is("f0,f1"));
    assertThat(conf3.get(FlinkOptions.KEYGEN_CLASS), is(NonpartitionedAvroKeyGenerator.class.getName()));
  }

  @Test
  void testSetupCleaningOptionsForSink() {
    // definition with simple primary key and partition path
    TableSchema schema1 = TableSchema.builder()
        .field("f0", DataTypes.INT().notNull())
        .field("f1", DataTypes.VARCHAR(20))
        .field("f2", DataTypes.TIMESTAMP(3))
        .field("ts", DataTypes.TIMESTAMP(3))
        .primaryKey("f0")
        .build();
    // set up new retains commits that is less than min archive commits
    this.conf.setString(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "11");

    final MockContext sinkContext1 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink1 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext1);
    final Configuration conf1 = tableSink1.getConf();
    assertThat(conf1.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), is(20));
    assertThat(conf1.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS), is(30));

    // set up new retains commits that is greater than min archive commits
    this.conf.setString(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "25");

    final MockContext sinkContext2 = MockContext.getInstance(this.conf, schema1, "f2");
    final HoodieTableSink tableSink2 = (HoodieTableSink) new HoodieTableFactory().createDynamicTableSink(sinkContext2);
    final Configuration conf2 = tableSink2.getConf();
    assertThat(conf2.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), is(35));
    assertThat(conf2.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS), is(45));
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Mock dynamic table factory context.
   */
  private static class MockContext implements DynamicTableFactory.Context {
    private final Configuration conf;
    private final TableSchema schema;
    private final List<String> partitions;

    private MockContext(Configuration conf, TableSchema schema, List<String> partitions) {
      this.conf = conf;
      this.schema = schema;
      this.partitions = partitions;
    }

    static MockContext getInstance(Configuration conf) {
      return getInstance(conf, TestConfigurations.TABLE_SCHEMA, Collections.singletonList("partition"));
    }

    static MockContext getInstance(Configuration conf, TableSchema schema, String partition) {
      return getInstance(conf, schema, Collections.singletonList(partition));
    }

    static MockContext getInstance(Configuration conf, TableSchema schema, List<String> partitions) {
      return new MockContext(conf, schema, partitions);
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
      return ObjectIdentifier.of("hudi", "default", "t1");
    }

    @Override
    public CatalogTable getCatalogTable() {
      return new CatalogTableImpl(schema, partitions, conf.toMap(), "mock source table");
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
