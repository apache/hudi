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

package org.apache.hudi.table.action;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

public abstract class BaseActionExecutor<T, I, K, O, R> implements Serializable {

  protected final transient HoodieEngineContext context;
  protected final transient Configuration hadoopConf;

  protected final HoodieWriteConfig config;

  protected final HoodieTable<T, I, K, O> table;

  protected final String instantTime;

  public BaseActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    this.context = context;
    this.hadoopConf = context.getHadoopConf().get();
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
  }

  public abstract R execute();

  /**
   * Writes commits metadata to table metadata.
   * @param metadata commit metadata of interest.
   */
  protected final void writeTableMetadata(HoodieCommitMetadata metadata, String actionType) {
    table.getMetadataWriter(instantTime).ifPresent(w -> w.update(
        metadata, instantTime, table.isTableServiceAction(actionType)));
  }

  /**
   * Writes clean metadata to table metadata.
   * @param metadata clean metadata of interest.
   */
  protected final void writeTableMetadata(HoodieCleanMetadata metadata, String instantTime) {
    table.getMetadataWriter(instantTime).ifPresent(w -> w.update(metadata, instantTime));
  }

  /**
   * Writes rollback metadata to table metadata.
   * @param metadata rollback metadata of interest.
   */
  protected final void writeTableMetadata(HoodieRollbackMetadata metadata) {
    table.getMetadataWriter(instantTime, Option.of(metadata)).ifPresent(w -> w.update(metadata, instantTime));
  }

  /**
   * Writes restore metadata to table metadata.
   * @param metadata restore metadata of interest.
   */
  protected final void writeTableMetadata(HoodieRestoreMetadata metadata) {
    table.getMetadataWriter(instantTime, Option.of(metadata)).ifPresent(w -> w.update(metadata, instantTime));
  }
}
