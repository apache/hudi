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
import org.apache.hudi.utils.factory.CollectSinkTableFactory;
import org.apache.hudi.utils.factory.ContinuousFileSourceFactory;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.Objects;

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

  public static final TableSchema TABLE_SCHEMA = TableSchema.builder()
      .fields(
          ROW_TYPE.getFieldNames().toArray(new String[0]),
          ROW_DATA_TYPE.getChildren().toArray(new DataType[0]))
      .build();

  public static final TypeInformation<Row> ROW_TYPE_INFO = Types.ROW(
      Types.STRING,
      Types.STRING,
      Types.INT,
      Types.LOCAL_DATE_TIME,
      Types.STRING);

  public static String getCreateHoodieTableDDL(String tableName, Map<String, String> options) {
    String createTable = "create table " + tableName + "(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi'";
    StringBuilder builder = new StringBuilder(createTable);
    if (options.size() != 0) {
      options.forEach((k, v) -> builder.append(",\n")
          .append("  '").append(k).append("' = '").append(v).append("'"));
    }
    builder.append("\n)");
    return builder.toString();
  }

  public static String getFileSourceDDL(String tableName) {
    return getFileSourceDDL(tableName, "test_source.data");
  }

  public static String getFileSourceDDL(String tableName, String fileName) {
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
        + "  'path' = '" + sourcePath + "'\n"
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

  public static final RowDataSerializer SERIALIZER = new RowDataSerializer(new ExecutionConfig(), ROW_TYPE);

  public static Configuration getDefaultConf(String tablePath) {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.READ_AVRO_SCHEMA_PATH,
        Objects.requireNonNull(Thread.currentThread()
            .getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    return conf;
  }

  public static FlinkStreamerConfig getDefaultStreamerConf(String tablePath) {
    FlinkStreamerConfig streamerConf = new FlinkStreamerConfig();
    streamerConf.targetBasePath = tablePath;
    streamerConf.readSchemaFilePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_read_schema.avsc")).toString();
    streamerConf.targetTableName = "TestHoodieTable";
    streamerConf.partitionPathField = "partition";
    streamerConf.tableType = "COPY_ON_WRITE";
    streamerConf.checkpointInterval = 4000L;
    return streamerConf;
  }
}
