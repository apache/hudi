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

package org.apache.hudi.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HudiSinkConnector is a Kafka Connect Connector implementation
 * that ingest data from Kafka to Hudi.
 */
public class HoodieSinkConnector extends SinkConnector {

  public static final String VERSION = "0.1.0";
  private static final Logger LOG = LogManager.getLogger(HoodieSinkConnector.class);
  private Map<String, String> configProps;

  /**
   * No-arg constructor. It is instantiated by Connect framework.
   */
  public HoodieSinkConnector() {
  }

  @Override
  public String version() {
    return VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = new HashMap<>(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HoodieSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Map<String, String> taskProps = new HashMap<>(configProps);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    LOG.info(String.format("Shutting down Hudi Sink connector %s", configProps.get("name")));
  }

  @Override
  public ConfigDef config() {
    // we use Hudi configs instead
    return new ConfigDef();
  }
}
