/*
 * <!--
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * -->
 *
 */

package org.apache.hudi.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.schema.FilebasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for Flink stream read and write.
 */
public class FlinkStreamerUtil {
  public static TypedProperties getProps(FlinkStreamerConfig cfg) {
    if (cfg.propsFilePath.isEmpty()) {
      return new TypedProperties();
    }
    return org.apache.hudi.util.StreamerUtil.readConfig(
        FSUtils.getFs(cfg.propsFilePath, org.apache.hudi.util.StreamerUtil.getHadoopConf()),
        new Path(cfg.propsFilePath), cfg.configs).getConfig();
  }

  public static TypedProperties appendKafkaProps(FlinkStreamerConfig config) {
    TypedProperties properties = getProps(config);
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.kafkaGroupId);
    return properties;
  }

  public static Schema getSourceSchema(FlinkStreamerConfig cfg) {
    return new FilebasedSchemaProvider(fromStreamerConfig(cfg)).getSourceSchema();
  }

  /**
   * Transforms a {@code HoodieFlinkStreamer.Config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  @SuppressWarnings("unchecked, rawtypes")
  public static org.apache.flink.configuration.Configuration fromStreamerConfig(FlinkStreamerConfig config) {
    Map<String, String> propsMap = new HashMap<String, String>((Map) getProps(config));
    org.apache.flink.configuration.Configuration conf = FlinkOptions.fromMap(propsMap);

    conf.setString(FlinkOptions.PATH, config.targetBasePath);
    conf.setString(FlinkOptions.READ_AVRO_SCHEMA_PATH, config.readSchemaFilePath);
    conf.setString(FlinkOptions.TABLE_NAME, config.targetTableName);
    // copy_on_write works same as COPY_ON_WRITE
    conf.setString(FlinkOptions.TABLE_TYPE, config.tableType.toUpperCase());
    conf.setString(FlinkOptions.OPERATION, config.operation.value());
    conf.setString(FlinkOptions.PRECOMBINE_FIELD, config.sourceOrderingField);
    conf.setString(FlinkOptions.PAYLOAD_CLASS, config.payloadClassName);
    conf.setBoolean(FlinkOptions.INSERT_DROP_DUPS, config.filterDupes);
    conf.setInteger(FlinkOptions.RETRY_TIMES, Integer.parseInt(config.instantRetryTimes));
    conf.setLong(FlinkOptions.RETRY_INTERVAL_MS, Long.parseLong(config.instantRetryInterval));
    conf.setBoolean(FlinkOptions.IGNORE_FAILED, config.commitOnErrors);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, config.recordKeyField);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, config.partitionPathField);
    conf.setString(FlinkOptions.KEYGEN_CLASS, config.keygenClass);
    conf.setInteger(FlinkOptions.WRITE_TASKS, config.writeTaskNum);

    return conf;
  }
}
