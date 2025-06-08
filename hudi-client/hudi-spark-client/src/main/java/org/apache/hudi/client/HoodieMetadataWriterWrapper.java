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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HoodieMetadataWriterWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataWriterWrapper.class);

  // Cached HoodieTableMetadataWriter for each action in data table. This will be cleaned up when action is completed or when write client is closed.
  protected Map<String, Option<HoodieTableMetadataWriter>> metadataWriterMap = new ConcurrentHashMap<>();

  private void writeToMetadataTable(HoodieTable table, String instantTime, List<HoodieWriteStat> metadataWriteStatsSoFar, HoodieCommitMetadata metadata) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    if (metadataWriterOpt.isPresent()) {
      try {
        metadataWriterOpt.get().completeStreamingCommit(instantTime, table.getContext(), metadataWriteStatsSoFar, metadata);
      } finally {
        closeMetadataWriter(metadataWriterOpt.get());
        metadataWriterMap.remove(instantTime);
      }
    } else {
      throw new HoodieException("Should not be reachable. Metadata Writer should have been instantiated by now");
    }
  }

  private void closeMetadataWriter(HoodieTableMetadataWriter metadataWriter) {
    try {
      metadataWriter.close();
    } catch (Exception e) {
      throw new HoodieException("Failed to close Metadata writer ", e);
    }
  }

  // to be called from WriteClient and TableServiceClient.
  public HoodieData<WriteStatus> mayBeStreamWriteToMetadataTable(HoodieTable table, HoodieWriteConfig config, Boolean isMetadataTable,
                                                                                         HoodieData<WriteStatus> writeStatuses, String instantTime) {
    if (!isMetadataTable && config.isMetadataTableEnabled() && config.isMetadataStreamingWritesEnabled(table.getMetaClient().getTableConfig().getTableVersion())) {
      return streamWriteToMetadataTable(table, writeStatuses, instantTime);
    } else {
      return writeStatuses;
    }
  }

  private HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieTable table, HoodieData<WriteStatus> dataTableWriteStatuses, String instantTime) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    if (metadataWriterOpt.isPresent()) {
      //metadataWriterOpt.get().startCommit(instantTime);
      return maybeStreamWriteToMetadataTable(dataTableWriteStatuses, metadataWriterOpt, table, instantTime);
    } else {
      // should we throw exception if we can't get a metadata writer?
      return dataTableWriteStatuses;
    }
  }

  // to be invoked by write client or table service client.
  public void writeToMetadataTable(HoodieTable table, HoodieWriteConfig config, boolean isMetadataTable, String instantTime,
                                      HoodieCommitMetadata metadata, List<HoodieWriteStat> metadataWriteStatsSoFar,
                                    BaseHoodieClient baseHoodieClient) {
    boolean streamingWritesToMetadataTableEnabled = !isMetadataTable && config.isMetadataTableEnabled()
        && config.isMetadataStreamingWritesEnabled(table.getMetaClient().getTableConfig().getTableVersion());
    if (streamingWritesToMetadataTableEnabled) {
      writeToMetadataTable(table, instantTime, metadataWriteStatsSoFar, metadata);
    } else {
      // legacy write DAG to metadata table.
      baseHoodieClient.writeTableMetadata(table, instantTime, metadata);
    }
  }

  /**
   * For new instant time,
   * @param triggeringInstantTimestamp
   * @param table
   * @return
   */
  private synchronized Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp, HoodieTable table) {

    if (!table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT) || this.metadataWriterMap == null) {
      return Option.empty();
    }

    if (this.metadataWriterMap.containsKey(triggeringInstantTimestamp)) {
      return this.metadataWriterMap.get(triggeringInstantTimestamp);
    }

    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(triggeringInstantTimestamp, true);
    metadataWriterMap.put(triggeringInstantTimestamp, metadataWriterOpt); // populate this for every new instant time.
    // if metadata table does not exist, the map will contain an entry, with value Option.empty.
    // if not, it will contain the metadata writer instance.

    // start the commit in metadata table.
    metadataWriterOpt.ifPresent(metadataWriter -> metadataWriter.startCommit(triggeringInstantTimestamp));

    return metadataWriterMap.get(triggeringInstantTimestamp);
  }

  private HoodieData<WriteStatus> maybeStreamWriteToMetadataTable(HoodieData<WriteStatus> dtWriteStatuses, Option<HoodieTableMetadataWriter> metadataWriterOpt,
                                                                  HoodieTable table, String instantTime) {
    HoodieData<WriteStatus> allWriteStatus = dtWriteStatuses;
    HoodieData<WriteStatus> mdtWriteStatuses = metadataWriterOpt.get().streamWriteToMetadataPartitions(dtWriteStatuses, instantTime);
    allWriteStatus = allWriteStatus.union(mdtWriteStatuses);
    allWriteStatus.persist("MEMORY_AND_DISK_SER", table.getContext(), HoodieData.HoodieDataCacheKey.of(table.getMetaClient().getBasePath().toString(), instantTime));
    return allWriteStatus;
  }
}
