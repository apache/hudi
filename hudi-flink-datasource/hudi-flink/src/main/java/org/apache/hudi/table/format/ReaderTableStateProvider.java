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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.io.Serializable;

/**
 * Supplies the {@link FileGroupReaderTableState} a Flink reader uses for each split, from table state captured
 * where the read is planned, so that readers do not build a meta client per split.
 *
 * <p>A bounded read reads every split against one snapshot taken with the splits. A streaming read keeps receiving
 * splits for instants that complete after planning, so each of its splits gets its own state that loads the
 * timeline on first use, which only log reads of tables before version 8 and schema-on-read need.
 */
public final class ReaderTableStateProvider implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StoragePath basePath;
  private final HoodieTableConfig tableConfig;
  // null for a streaming read
  private final FileGroupReaderTableState snapshot;

  private ReaderTableStateProvider(StoragePath basePath, HoodieTableConfig tableConfig, FileGroupReaderTableState snapshot) {
    this.basePath = basePath;
    this.tableConfig = tableConfig;
    this.snapshot = snapshot;
  }

  /**
   * Captures one snapshot for all splits of a bounded read, see {@link FileGroupReaderTableState#snapshotOf}.
   */
  public static ReaderTableStateProvider snapshotOf(HoodieTableMetaClient metaClient, boolean withSchemaHistory) {
    FileGroupReaderTableState snapshot = FileGroupReaderTableState.snapshotOf(metaClient, withSchemaHistory);
    return new ReaderTableStateProvider(snapshot.getBasePath(), snapshot.getTableConfig(), snapshot);
  }

  /**
   * Captures the table config for a streaming read, whose splits load the timeline themselves when they need it.
   */
  public static ReaderTableStateProvider perSplit(HoodieTableMetaClient metaClient) {
    return new ReaderTableStateProvider(metaClient.getBasePath(), metaClient.getTableConfig(), null);
  }

  public HoodieTableConfig getTableConfig() {
    return tableConfig;
  }

  /**
   * Returns the state to read one split with.
   *
   * @param storageConf the storage configuration of the reader, used if the state has to load the timeline
   */
  public FileGroupReaderTableState forSplit(StorageConfiguration<?> storageConf) {
    if (snapshot != null) {
      return snapshot;
    }
    String tablePath = basePath.toString();
    return FileGroupReaderTableState.withLazyTimeline(basePath, tableConfig,
        () -> HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(storageConf.newInstance()).build());
  }
}
