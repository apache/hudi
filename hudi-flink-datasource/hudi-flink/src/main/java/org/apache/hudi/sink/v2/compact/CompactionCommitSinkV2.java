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

package org.apache.hudi.sink.v2.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.handler.CompactionCommitHandler;
import org.apache.hudi.sink.compact.handler.TableServiceHandlerFactory;
import org.apache.hudi.sink.v2.CleanFunctionV2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;

/**
 * Function to check and commit the compaction action.
 *
 * <p> Each time after receiving a compaction commit event {@link CompactionCommitEvent},
 * it loads and checks the compaction plan {@link HoodieCompactionPlan},
 * if all the compaction operations {@link org.apache.hudi.common.model.CompactionOperation}
 * of the plan are finished, tries to commit the compaction action.
 *
 * <p>It also inherits the {@link CleanFunctionV2} cleaning ability. This is needed because
 * the SQL API does not allow multiple sinks in one table sink provider.
 *
 * <p>Note: The difference with {@code CompactionCommitSink} is {@code CompactionCommitSinkV2}
 * extends {@code ProcessFunction}, while {@code CompactionCommitSink} is a {@code SinkFunction}.
 */
public class CompactionCommitSinkV2 extends CleanFunctionV2<CompactionCommitEvent> {

  /**
   * Config options.
   */
  private final Configuration conf;

  private transient CompactionCommitHandler compactCommitHandler;

  public CompactionCommitSinkV2(Configuration conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.compactCommitHandler = TableServiceHandlerFactory.createCompactionCommitHandler(conf, getRuntimeContext());
    this.compactCommitHandler.registerMetrics(getRuntimeContext().getMetricGroup());
  }

  @Override
  public void processElement(
      CompactionCommitEvent event,
      ProcessFunction<CompactionCommitEvent, RowData>.Context context,
      Collector<RowData> collector) throws Exception {
    compactCommitHandler.commitIfNecessary(event);
  }

  @Override
  public void close() throws Exception {
    IOUtils.closeAll(compactCommitHandler, super::close);
  }
}
