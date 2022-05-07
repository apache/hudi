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

package org.apache.hudi.utils;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;
import org.apache.hudi.utils.factory.ContinuousFileSourceFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Configurations for the test.
 */
public class TestConfigurations {
  private TestConfigurations() {
  }

  public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
          DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
          DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
          DataTypes.FIELD("age", DataTypes.INT()),
          DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
          DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
      .notNull();

  public static final RowType ROW_TYPE = (RowType) ROW_DATA_TYPE.getLogicalType();

  public static final ResolvedSchema TABLE_SCHEMA = SchemaBuilder.instance()
      .fields(ROW_TYPE.getFieldNames(), ROW_DATA_TYPE.getChildren())
      .build();

  private static final List<String> FIELDS = ROW_TYPE.getFields().stream()
      .map(RowType.RowField::asSummaryString).collect(Collectors.toList());

  public static final DataType ROW_DATA_TYPE_WIDER = DataTypes.ROW(
          DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
          DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
          DataTypes.FIELD("age", DataTypes.INT()),
          DataTypes.FIELD("salary", DataTypes.DOUBLE()),
          DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
          DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
      .notNull();

  public static final RowType ROW_TYPE_WIDER = (RowType) ROW_DATA_TYPE_WIDER.getLogicalType();

  public static final DataType ROW_DATA_TYPE_DATE = DataTypes.ROW(
          DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
          DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
          DataTypes.FIELD("age", DataTypes.INT()),
          DataTypes.FIELD("dt", DataTypes.DATE()))
      .notNull();

  public static final RowType ROW_TYPE_DATE = (RowType) ROW_DATA_TYPE_DATE.getLogicalType();

  public static String getCreateHoodieTableDDL(String tableName, Map<String, String> options) {
    return getCreateHoodieTableDDL(tableName, options, true, "partition");
  }

  public static String getCreateHoodieTableDDL(
      String tableName,
      Map<String, String> options,
      boolean havePartition,
      String partitionField) {
    return getCreateHoodieTableDDL(tableName, FIELDS, options, havePartition, "uuid", partitionField);
  }

  public static String getCreateHoodieTableDDL(
      String tableName,
      List<String> fields,
      Map<String, String> options,
      boolean havePartition,
      String pkField,
      String partitionField) {
    StringBuilder builder = new StringBuilder();
    builder.append("create table ").append(tableName).append("(\n");
    for (String field : fields) {
      builder.append("  ").append(field).append(",\n");
    }
    builder.append("  PRIMARY KEY(").append(pkField).append(") NOT ENFORCED\n")
        .append(")\n");
    if (havePartition) {
      builder.append("PARTITIONED BY (`").append(partitionField).append("`)\n");
    }
    final String connector = options.computeIfAbsent("connector", k -> "hudi");
    builder.append("with (\n"
        + "  'connector' = '").append(connector).append("'");
    options.forEach((k, v) -> builder.append(",\n")
        .append("  '").append(k).append("' = '").append(v).append("'"));
    builder.append("\n)");
    return builder.toString();
  }

  public static String getCreateHudiCatalogDDL(final String catalogName, final String catalogPath) {
    StringBuilder builder = new StringBuilder();
    builder.append("create catalog ").append(catalogName).append(" with (\n");
    builder.append("  'type' = 'hudi',\n"
        + "  'catalog.path' = '").append(catalogPath).append("'");
    builder.append("\n)");
    return builder.toString();
  }

  public static String getFileSourceDDL(String tableName) {
    return getFileSourceDDL(tableName, "test_source.data");
  }

  public static String getFileSourceDDL(String tableName, int checkpoints) {
    return getFileSourceDDL(tableName, "test_source.data", checkpoints);
  }

  public static String getFileSourceDDL(String tableName, String fileName) {
    return getFileSourceDDL(tableName, fileName, 2);
  }

  public static String getFileSourceDDL(String tableName, String fileName, int checkpoints) {
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource(fileName)).toString();
    return "create table " + tableName + "(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20)\n"
        + ") with (\n"
        + "  'connector' = '" + ContinuousFileSourceFactory.FACTORY_ID + "',\n"
        + "  'path' = '" + sourcePath + "',\n"
        + "  'checkpoints' = '" + checkpoints + "'\n"
        + ")";
  }

  public static String getCollectSinkDDL(String tableName) {
    return "create table " + tableName + "(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20)\n"
        + ") with (\n"
        + "  'connector' = '" + CollectSinkTableFactory.FACTORY_ID + "'"
        + ")";
  }

  public static String getCollectSinkDDL(String tableName, TableSchema tableSchema) {
    final StringBuilder builder = new StringBuilder("create table " + tableName + "(\n");
    String[] fieldNames = tableSchema.getFieldNames();
    DataType[] fieldTypes = tableSchema.getFieldDataTypes();
    for (int i = 0; i < fieldNames.length; i++) {
      builder.append("  `")
          .append(fieldNames[i])
          .append("` ")
          .append(fieldTypes[i].toString());
      if (i != fieldNames.length - 1) {
        builder.append(",");
      }
      builder.append("\n");
    }
    final String withProps = ""
        + ") with (\n"
        + "  'connector' = '" + CollectSinkTableFactory.FACTORY_ID + "'\n"
        + ")";
    builder.append(withProps);
    return builder.toString();
  }

  public static String getCsvSourceDDL(String tableName, String fileName) {
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource(fileName)).toString();
    return "create table " + tableName + "(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20)\n"
        + ") with (\n"
        + "  'connector' = 'filesystem',\n"
        + "  'path' = '" + sourcePath + "',\n"
        + "  'format' = 'csv'\n"
        + ")";
  }

  public static final RowDataSerializer SERIALIZER = new RowDataSerializer(ROW_TYPE);

  public static Configuration getDefaultConf(String tablePath) {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH,
        Objects.requireNonNull(Thread.currentThread()
            .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    return conf;
  }

  public static Configuration getDefaultConf(String tablePath, DataType dataType) {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, AvroSchemaConverter.convertToSchema(dataType.getLogicalType()).toString());
    conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    return conf;
  }

  public static FlinkStreamerConfig getDefaultStreamerConf(String tablePath) {
    FlinkStreamerConfig streamerConf = new FlinkStreamerConfig();
    streamerConf.targetBasePath = tablePath;
    streamerConf.sourceAvroSchemaPath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_read_schema.avsc")).toString();
    streamerConf.targetTableName = "TestHoodieTable";
    streamerConf.partitionPathField = "partition";
    streamerConf.tableType = "COPY_ON_WRITE";
    streamerConf.checkpointInterval = 4000L;
    return streamerConf;
  }

  /**
   * Creates the tool to build hoodie table DDL.
   */
  public static Sql sql(String tableName) {
    return new Sql(tableName);
  }

  public static Catalog catalog(String catalogName) {
    return new Catalog(catalogName);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Tool to build hoodie table DDL with schema {@link #TABLE_SCHEMA}.
   */
  public static class Sql {
    private final Map<String, String> options;
    private final String tableName;
    private List<String> fields = new ArrayList<>();
    private boolean withPartition = true;
    private String pkField = "uuid";
    private String partitionField = "partition";

    public Sql(String tableName) {
      options = new HashMap<>();
      this.tableName = tableName;
    }

    public Sql option(ConfigOption<?> option, Object val) {
      this.options.put(option.key(), val.toString());
      return this;
    }

    public Sql option(String key, Object val) {
      this.options.put(key, val.toString());
      return this;
    }

    public Sql options(Map<String, String> options) {
      this.options.putAll(options);
      return this;
    }

    public Sql noPartition() {
      this.withPartition = false;
      return this;
    }

    public Sql pkField(String pkField) {
      this.pkField = pkField;
      return this;
    }

    public Sql partitionField(String partitionField) {
      this.partitionField = partitionField;
      return this;
    }

    public Sql field(String fieldSchema) {
      fields.add(fieldSchema);
      return this;
    }

    public String end() {
      if (this.fields.size() == 0) {
        this.fields = FIELDS;
      }
      return TestConfigurations.getCreateHoodieTableDDL(this.tableName, this.fields, options,
          this.withPartition, this.pkField, this.partitionField);
    }
  }

  public static class Catalog {
    private final String catalogName;
    private String catalogPath = ".";

    public Catalog(String catalogName) {
      this.catalogName = catalogName;
    }

    public Catalog catalogPath(String catalogPath) {
      this.catalogPath = catalogPath;
      return this;
    }

    public String end() {
      return TestConfigurations.getCreateHudiCatalogDDL(catalogName, catalogPath);
    }
  }
}
