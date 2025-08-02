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

package org.apache.hudi.table.format;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.function.Supplier;

/**
 * Factory for creating a Flink-specific reader context.
 * This context is used specifically for table services such as compaction and clustering.
 */
public class FlinkReaderContextFactory implements ReaderContextFactory<RowData> {
  private final HoodieTableMetaClient metaClient;
  private InternalSchemaManager internalSchemaManager;
  private boolean shouldUseMetaFields;

  public FlinkReaderContextFactory(HoodieTableMetaClient metaClient, Boolean shouldUseMetaFields) {
    this.metaClient = metaClient;
    this.shouldUseMetaFields = shouldUseMetaFields;
  }

  @Override
  public HoodieReaderContext<RowData> getContext() {
    Supplier<InternalSchemaManager> internalSchemaManagerSupplier = () -> {
      // CAUTION: instantiate internalSchemaManager lazily here since it may not be needed for FG reader,
      // e.g., schema evolution for log files in FG reader do not use internalSchemaManager.
      if (internalSchemaManager == null) {
        internalSchemaManager = InternalSchemaManager.get(metaClient.getStorageConf(), metaClient);
      }
      return internalSchemaManager;
    };
    return new FlinkRowDataReaderContext(
        metaClient.getStorageConf(), internalSchemaManagerSupplier, Collections.emptyList(), metaClient.getTableConfig(), Option.empty(), shouldUseMetaFields);
  }
}
