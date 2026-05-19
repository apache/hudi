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

package org.apache.hudi.source.reader.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkWriteClients;

import java.util.List;

/**
 * Abstract implementation of SplitReaderFunction.
 */
public abstract class AbstractSplitReaderFunction implements SplitReaderFunction<RowData> {

  protected final Configuration conf;
  protected final InternalSchemaManager internalSchemaManager;
  protected final List<ExpressionPredicates.Predicate> predicates;
  protected final boolean emitDelete;
  private transient HoodieWriteConfig writeConfig;
  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  public AbstractSplitReaderFunction(
      Configuration conf,
      List<ExpressionPredicates.Predicate> predicates,
      InternalSchemaManager internalSchemaManager,
      boolean emitDelete) {
    this.conf = conf;
    this.predicates = predicates;
    this.internalSchemaManager = internalSchemaManager;
    this.emitDelete = emitDelete;
  }

  protected HoodieWriteConfig getWriteConfig() {
    if (writeConfig == null) {
      writeConfig = FlinkWriteClients.getHoodieClientConfig(conf);
    }
    return writeConfig;
  }

  protected org.apache.hadoop.conf.Configuration getHadoopConf() {
    if (hadoopConf == null) {
      hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    }
    return hadoopConf;
  }
}
