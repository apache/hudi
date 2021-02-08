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

package org.apache.hudi.operator.utils;

import org.apache.hudi.operator.FlinkOptions;
import org.apache.hudi.streamer.FlinkStreamerConfig;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * Configurations for the test.
 */
public class TestConfigurations {
  private TestConfigurations() {
  }

  public static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
      DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
      DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("age", DataTypes.INT()),
      DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
      DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
      .notNull()
      .getLogicalType();

  public static final RowDataSerializer SERIALIZER = new RowDataSerializer(new ExecutionConfig(), ROW_TYPE);

  public static Configuration getDefaultConf(String tablePath) {
    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.READ_SCHEMA_FILE_PATH,
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
