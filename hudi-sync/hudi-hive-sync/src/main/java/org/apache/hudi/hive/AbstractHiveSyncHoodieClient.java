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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.hudi.sync.common.HoodieSyncException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class to sync Hudi tables with Hive based metastores, such as Hive server, HMS or managed Hive services.
 */
public abstract class AbstractHiveSyncHoodieClient extends AbstractSyncHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractHiveSyncHoodieClient.class);

  public AbstractHiveSyncHoodieClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata, boolean verifyMetadataFileListing, boolean withOperationField,
                                      FileSystem fs) {
    super(basePath, assumeDatePartitioning, useFileListingFromMetadata, verifyMetadataFileListing, withOperationField, fs);
  }

  public AbstractHiveSyncHoodieClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata, boolean withOperationField, FileSystem fs) {
    super(basePath, assumeDatePartitioning, useFileListingFromMetadata, withOperationField, fs);
  }

  public abstract void createDatabase();

  /**
   * @return true if the configured database exists
   */
  public abstract boolean databaseExists();

  public abstract void updateSchema(String tableName, MessageType newSchema);

  public abstract List<PartitionEvent> getPartitionEvents(String tableName, List<String> writtenPartitionsSince);

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds it or if the
   * partition path does not match, it updates the partition path).
   */
  public boolean syncPartitions(String tableName, List<String> writtenPartitionsSince) {
    boolean partitionsChanged;
    try {
      List<AbstractSyncHoodieClient.PartitionEvent> partitionEvents =
          getPartitionEvents(tableName, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType.ADD);
      LOG.info("New Partitions " + newPartitions);
      addPartitionsToTable(tableName, newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType.UPDATE);
      LOG.info("Changed Partitions " + updatePartitions);
      updatePartitionsToTable(tableName, updatePartitions);
      partitionsChanged = !updatePartitions.isEmpty() || !newPartitions.isEmpty();
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to sync partitions for table " + tableName
          + " in basepath " + getBasePath(), e);
    }

    return partitionsChanged;
  }

  private List<String> filterPartitions(List<AbstractSyncHoodieClient.PartitionEvent> events, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType).map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }
}

