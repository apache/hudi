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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Class to assist with streaming writes to metadata table.
 */
@Slf4j
public class FlinkStreamingMetadataWriteHandler extends StreamingMetadataWriteHandler {

  @Override
  public HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieTable table,
                                                            HoodieData<WriteStatus> dataTableWriteStatuses,
                                                            String instantTime,
                                                            int coalesceDivisorForDataTableWrites) {
    throw new UnsupportedOperationException("Not implemented since it's not needed for flink engine.");
  }

  @Override
  protected void startCommit(Option<HoodieTableMetadataWriter> metadataWriterOpt, String triggeringInstant) {
    // The flink writer has customized instant starting logic in the writing operatorCoordinator,
    // here do nothing when creating a new metadata writer.
  }

  /**
   * Performs streaming write operations to metadata partitions.
   * This method retrieves the metadata writer for the given instant time and table,
   * validates that it exists, and then performs streaming writes of index records.
   *
   * @param table          The hoodie table to write metadata for
   * @param indexRecords   The hoodie records containing index information to be written
   * @param dataPartitions The set of updated partitions of data table
   * @param instantTime    The instant time for the write operation
   *
   * @return HoodieData containing the write statuses of the operation
   */
  HoodieData<WriteStatus> streamWriteToMetadataPartitions(HoodieTable table, HoodieData<HoodieRecord> indexRecords, Set<String> dataPartitions, String instantTime) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    ValidationUtils.checkState(metadataWriterOpt.isPresent(),
        "Cannot instantiate metadata writer for the table of interest " + table.getMetaClient().getBasePath());
    return metadataWriterOpt.get().streamWriteToMetadataPartitions(indexRecords, dataPartitions, instantTime);
  }

  /**
   * Start the commit in metadata table with given instant.
   *
   * @param instantTime The instant that triggers the metadata writes.
   * @param table       The hoodie table
   */
  public void startCommit(String instantTime, HoodieTable table) {
    // will start commit when first create the metadata writer.
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    ValidationUtils.checkState(metadataWriterOpt.isPresent(), "Should not be reachable. Metadata Writer should have been instantiated by now");
    // start the commit in metadata table.
    metadataWriterOpt.get().startCommit(instantTime);
  }

  /**
   * Clean resources after streaming write to the metadata table in index write function or stop
   * heartbeat for instant in the coordinator. This method removes the metadata writer associated
   * with the given instant time from the internal map and closes it if it exists.
   *
   * @param instantTime Instant Time
   */
  public void cleanResources(String instantTime) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = this.metadataWriterMap.remove(instantTime);
    if (metadataWriterOpt == null || metadataWriterOpt.isEmpty()) {
      log.warn("Metadata writer for {} has not been initialized, no need to stop heartbeat.", instantTime);
      return;
    }
    try {
      metadataWriterOpt.get().close();
    } catch (Exception e) {
      throw new HoodieException("Failed to close the metadata writer for instant: " + instantTime, e);
    }
  }
}
