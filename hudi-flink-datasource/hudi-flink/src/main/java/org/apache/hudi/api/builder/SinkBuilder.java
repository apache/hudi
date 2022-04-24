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

import org.apache.hudi.api.util.HoodieSourceSinkUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * builder to build a data stream sink.
 */
public class SinkBuilder {

  public static SinkBuilder builder() {
    return new SinkBuilder();
  }

  private DataStream<Row> input;
  private Map<String, String> options;
  private ResolvedSchema schema;
  private List<String> partitions;
  boolean isBouned;

  public SinkBuilder self() {
    return this;
  }

  public SinkBuilder input(DataStream<Row> input) {
    this.input = input;
    return self();
  }

  public SinkBuilder options(Map<String, String> options) {
    this.options = options;
    return self();
  }

  public SinkBuilder schema(ResolvedSchema schema) {
    this.schema = schema;
    return self();
  }

  public SinkBuilder partitions(List<String> partitions) {
    this.partitions = partitions;
    return self();
  }

  public SinkBuilder isBouned(boolean isBouned) {
    this.isBouned = isBouned;
    return self();
  }

  public DataStreamSink sink() {
    return HoodieSourceSinkUtil.builtHoodieSink(this.input, this.options, this.schema, this.partitions, this.isBouned);
  }

}
