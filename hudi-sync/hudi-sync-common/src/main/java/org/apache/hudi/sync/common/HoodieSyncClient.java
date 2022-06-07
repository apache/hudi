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

package org.apache.hudi.sync.common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

public abstract class HoodieSyncClient implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(HoodieSyncClient.class);

  public static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";

  protected final HoodieTableMetaClient metaClient;
  protected final HoodieTableType tableType;
  protected final FileSystem fs;
  private final String basePath;
  private final boolean assumeDatePartitioning;
  private final boolean useFileListingFromMetadata;
  private final boolean withOperationField;

  @Deprecated
  public HoodieSyncClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata,
                          boolean verifyMetadataFileListing, boolean withOperationField, FileSystem fs) {
    this(basePath, assumeDatePartitioning, useFileListingFromMetadata, withOperationField, fs);
  }

  public HoodieSyncClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata,
                          boolean withOperationField, FileSystem fs) {
    this.metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    this.tableType = metaClient.getTableType();
    this.basePath = basePath;
    this.assumeDatePartitioning = assumeDatePartitioning;
    this.useFileListingFromMetadata = useFileListingFromMetadata;
    this.withOperationField = withOperationField;
    this.fs = fs;
  }

  public abstract Map<String, String> getTableSchema(String tableName);

  public HoodieTableType getTableType() {
    return tableType;
  }

  public String getBasePath() {
    return metaClient.getBasePath();
  }

  public FileSystem getFs() {
    return fs;
  }

  public boolean isBootstrap() {
    return metaClient.getTableConfig().getBootstrapBasePath().isPresent();
  }

  public void closeQuietly(ResultSet resultSet, Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      LOG.warn("Could not close the statement opened ", e);
    }

    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOG.warn("Could not close the resultset opened ", e);
    }
  }

  /**
   * Gets the schema for a hoodie table. Depending on the type of table, try to read schema from commit metadata if
   * present, else fallback to reading from any file written in the latest commit. We will assume that the schema has
   * not changed within a single atomic write.
   *
   * @return Parquet schema for this table
   */
  public MessageType getDataSchema() {
    try {
      return new TableSchemaResolver(metaClient).getTableParquetSchema();
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read data schema", e);
    }
  }

  public boolean isDropPartition() {
    try {
      Option<HoodieCommitMetadata> hoodieCommitMetadata = HoodieTableMetadataUtil.getLatestCommitMetadata(metaClient);

      if (hoodieCommitMetadata.isPresent()
          && WriteOperationType.DELETE_PARTITION.equals(hoodieCommitMetadata.get().getOperationType())) {
        return true;
      }
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to get commit metadata", e);
    }
    return false;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public List<String> getPartitionsWrittenToSince(Option<String> lastCommitTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      LOG.info("Last commit time synced is not known, listing all partitions in " + basePath + ",FS :" + fs);
      HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
      return FSUtils.getAllPartitionPaths(engineContext, basePath, useFileListingFromMetadata, assumeDatePartitioning);
    } else {
      LOG.info("Last commit time synced is " + lastCommitTimeSynced.get() + ", Getting commits since then");
      return TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline().getCommitsTimeline()
          .findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE));
    }
  }

  /**
   * Partition Event captures any partition that needs to be added or updated.
   */
  public static class PartitionEvent {

    public enum PartitionEventType {
      ADD, UPDATE, DROP
    }

    public PartitionEventType eventType;
    public String storagePartition;

    PartitionEvent(PartitionEventType eventType, String storagePartition) {
      this.eventType = eventType;
      this.storagePartition = storagePartition;
    }

    public static PartitionEvent newPartitionAddEvent(String storagePartition) {
      return new PartitionEvent(PartitionEventType.ADD, storagePartition);
    }

    public static PartitionEvent newPartitionUpdateEvent(String storagePartition) {
      return new PartitionEvent(PartitionEventType.UPDATE, storagePartition);
    }

    public static PartitionEvent newPartitionDropEvent(String storagePartition) {
      return new PartitionEvent(PartitionEventType.DROP, storagePartition);
    }
  }
}
