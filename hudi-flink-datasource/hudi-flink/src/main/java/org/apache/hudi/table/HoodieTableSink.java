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

import org.apache.hudi.adapter.DataStreamSinkProviderAdapter;
import org.apache.hudi.adapter.SupportsRowLevelDeleteAdapter;
import org.apache.hudi.adapter.SupportsRowLevelUpdateAdapter;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.DataModificationInfos;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements
    DynamicTableSink,
    SupportsPartitioning,
    SupportsOverwrite,
    SupportsRowLevelDeleteAdapter,
    SupportsRowLevelUpdateAdapter {

  private final Configuration conf;
  private final ResolvedSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, ResolvedSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  public HoodieTableSink(Configuration conf, ResolvedSchema schema, boolean overwrite) {
    this.conf = conf;
    this.schema = schema;
    this.overwrite = overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    RowType rowType = (RowType) schema.toSinkRowDataType().notNull().getLogicalType();
    return (DataStreamSinkProviderAdapter) dataStream -> {
      // setup configuration
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
      // set up default parallelism
      OptionsInference.setupSinkTasks(conf, dataStream.getExecutionConfig().getParallelism());
      // set up client id
      OptionsInference.setupClientId(conf);
      // set up index related configs
      OptionsInference.setupIndexConfigs(conf);

      if (OptionsResolver.isSinkV2Enabled(conf)) {
        return Pipelines.sinkV2(dataStream, conf, rowType, overwrite, context.isBounded());
      } else {
        return Pipelines.sink(dataStream, conf, rowType, overwrite, context.isBounded());
      }
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    if (conf.get(FlinkOptions.CHANGELOG_ENABLED)) {
      return ChangelogModes.FULL;
    } else {
      return ChangelogModes.UPSERT;
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.schema, this.overwrite);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partitions) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite && !partitions.isEmpty()) {
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    }
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    if (OptionsResolver.overwriteDynamicPartition(conf)) {
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    } else {
      // if there are explicit partitions, #applyStaticPartition would overwrite the option.
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE_TABLE.value());
    }
  }

  @Override
  public RowLevelDeleteInfoAdapter applyRowLevelDelete() {
    this.conf.set(FlinkOptions.OPERATION, WriteOperationType.DELETE.value());
    return DataModificationInfos.DEFAULT_DELETE_INFO;
  }

  @Override
  public RowLevelUpdateInfoAdapter applyRowLevelUpdate(List<Column> list) {
    this.conf.set(FlinkOptions.OPERATION, WriteOperationType.UPSERT.value());
    return DataModificationInfos.DEFAULT_UPDATE_INFO;
  }
}
