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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.hudi.sync.common.HoodieSyncException;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class to sync Hudi tables with Hive based metastores, such as Hive server, HMS or managed Hive services.
 */
public abstract class AbstractHiveSyncHoodieClient extends AbstractSyncHoodieClient {

  protected final HoodieTimeline activeTimeline;
  protected final HiveSyncConfig syncConfig;
  protected final Configuration hadoopConf;
  protected final PartitionValueExtractor partitionValueExtractor;

  public AbstractHiveSyncHoodieClient(HiveSyncConfig syncConfig, Configuration hadoopConf, FileSystem fs) {
    super(syncConfig.basePath, syncConfig.assumeDatePartitioning, syncConfig.useFileListingFromMetadata, syncConfig.withOperationField, fs);
    this.syncConfig = syncConfig;
    this.hadoopConf = hadoopConf;
    this.partitionValueExtractor = ReflectionUtils.loadClass(syncConfig.partitionValueExtractorClass);
    this.activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public HoodieTimeline getActiveTimeline() {
    return activeTimeline;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  protected List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions, boolean isDropPartition) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);

      if (isDropPartition) {
        events.add(PartitionEvent.newPartitionDropEvent(storagePartition));
      } else {
        if (!storagePartitionValues.isEmpty()) {
          String storageValue = String.join(", ", storagePartitionValues);
          if (!paths.containsKey(storageValue)) {
            events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
          } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
            events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
          }
        }
      }
    }
    return events;
  }

  /**
   * Get all partitions for the table in the metastore.
   */
  public abstract List<Partition> getAllPartitions(String tableName);

  /**
   * Check if a database already exists in the metastore.
   */
  public abstract boolean databaseExists(String databaseName);

  /**
   * Create a database in the metastore.
   */
  public abstract void createDatabase(String databaseName);

  /**
   * Update schema for the table in the metastore.
   */
  public abstract void updateTableDefinition(String tableName, MessageType newSchema);

  /*
   * APIs below need to be re-worked by modeling field comment in hudi-sync-common,
   * instead of relying on Avro or Hive schema class.
   */

  public Schema getAvroSchemaWithoutMetadataFields() {
    try {
      return new TableSchemaResolver(metaClient).getTableAvroSchemaWithoutMetadataFields();
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read avro schema", e);
    }
  }

  public abstract List<FieldSchema> getTableCommentUsingMetastoreClient(String tableName);

  public abstract void updateTableComments(String tableName, List<FieldSchema> oldSchema, List<Schema.Field> newSchema);

  public abstract void updateTableComments(String tableName, List<FieldSchema> oldSchema, Map<String, String> newComments);

  /*
   * APIs above need to be re-worked by modeling field comment in hudi-sync-common,
   * instead of relying on Avro or Hive schema class.
   */
}
