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

import org.apache.hudi.api.HoodieSink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final HoodieSink hoodieSink;

  public HoodieTableSink(HoodieSink hoodieSink) {
    this.hoodieSink = hoodieSink;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return hoodieSink.getSinkRuntimeProvider(context.isBounded());
  }

  @VisibleForTesting
  public Configuration getConf() {
    return hoodieSink.getConf();
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return hoodieSink.getChangelogMode(changelogMode);
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.hoodieSink);
  }

  @Override
  public String asSummaryString() {
    return hoodieSink.asSummaryString();
  }

  @Override
  public void applyStaticPartition(Map<String, String> partitions) {
    hoodieSink.applyStaticPartition(partitions);
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    hoodieSink.applyOverwrite(overwrite);
  }
}
