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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstraction for data table write client and table service client to write to metadata table.
 */
public class HoodieMetadataWriteWrapper {

  // Cached HoodieTableMetadataWriter for each action in data table. This will be cleaned up when action is completed or when write client is closed.
  protected final Map<String, Option<HoodieTableMetadataWriter>> metadataWriterMap = new HashMap<>();

  /**
   * Called by data table write client and data table table service client to perform streaming write to metadata table.
   * @param table {@link HoodieTable} instance for data table of interest.
   * @param dataTableWriteStatuses {@link WriteStatus} from data table writes.
   * @param instantTime instant time of interest.
   * @return {@link HoodieData} of {@link WriteStatus} referring to both data table writes and partial metadata table writes.
   */
  public HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieTable table, HoodieData<WriteStatus> dataTableWriteStatuses, String instantTime) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    if (metadataWriterOpt.isPresent()) {
      return streamWriteToMetadataTable(dataTableWriteStatuses, metadataWriterOpt, table, instantTime);
    } else {
      throw new HoodieMetadataException("Cannot instantiate metadata writer for the table of interest " + table.getMetaClient().getBasePath());
    }
  }

  /**
   * To be invoked by write client or table service client to write to metadata table.
   * When streaming writes are enabled, writes to left over metadata partitions which was not covered in {@link #streamWriteToMetadataTable(HoodieTable, HoodieData, String)}
   * will be invoked here.
   * If not, writes take the legacy way of writing to metadata table.
   * @param table {@link HoodieTable} instance for data table of interest.
   * @param instantTime instant time of interest.
   * @param metadata {@link HoodieCommitMetadata} of interest.
   * @param metadataWriteStatsSoFar List of {@link HoodieWriteStat}s referring to partial writes completed in metadata table with streaming writes.
   */
  public void writeToMetadataTable(HoodieTable table, String instantTime,
                                   HoodieCommitMetadata metadata, List<HoodieWriteStat> metadataWriteStatsSoFar) {
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

  private HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieData<WriteStatus> dtWriteStatuses, Option<HoodieTableMetadataWriter> metadataWriterOpt,
                                                             HoodieTable table, String instantTime) {
    HoodieData<WriteStatus> allWriteStatus = dtWriteStatuses;
    HoodieData<WriteStatus> mdtWriteStatuses = metadataWriterOpt.get().streamWriteToMetadataPartitions(dtWriteStatuses, instantTime);
    allWriteStatus = allWriteStatus.union(mdtWriteStatuses);
    allWriteStatus.persist("MEMORY_AND_DISK_SER", table.getContext(), HoodieData.HoodieDataCacheKey.of(table.getMetaClient().getBasePath().toString(), instantTime));
    return allWriteStatus;
  }

  private void closeMetadataWriter(HoodieTableMetadataWriter metadataWriter) {
    try {
      metadataWriter.close();
    } catch (Exception e) {
      throw new HoodieException("Failed to close Metadata writer ", e);
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
}
