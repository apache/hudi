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

package org.apache.hudi.api.builder;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.hudi.api.util.HoodieSourceSinkUtil;

import java.util.List;
import java.util.Map;

public class SourceBuilder {
  public static  SourceBuilder builder() {
    return new SourceBuilder();
  }

  private StreamExecutionEnvironment environment;
  private Map<String, String> options;
  private ResolvedSchema schema;
  private List<String> partitions;

  public SourceBuilder self() {
    return this;
  }

  public SourceBuilder env(StreamExecutionEnvironment environment) {
    this.environment = environment;
    return self();
  }

  public SourceBuilder options(Map<String, String> options) {
    this.options = options;
    return self();
  }

  public SourceBuilder schema(ResolvedSchema schema) {
    this.schema = schema;
    return self();
  }

  public SourceBuilder partitions(List<String> partitions) {
    this.partitions = partitions;
    return self();
  }

  public DataStream<Row> source() {
    return HoodieSourceSinkUtil.builtHoodieSource(this.environment, this.options, this.schema, this.partitions);
  }
}
