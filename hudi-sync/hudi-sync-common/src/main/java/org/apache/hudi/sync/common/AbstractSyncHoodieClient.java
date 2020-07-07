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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractSyncHoodieClient {
  private static final Logger LOG = LogManager.getLogger(AbstractSyncHoodieClient.class);
  protected final HoodieTableMetaClient metaClient;
  protected HoodieTimeline activeTimeline;
  protected final HoodieTableType tableType;
  protected final FileSystem fs;
  private String basePath;
  private boolean assumeDatePartitioning;

  public AbstractSyncHoodieClient(String basePath, boolean assumeDatePartitioning, FileSystem fs) {
    this.metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
    this.tableType = metaClient.getTableType();
    this.basePath = basePath;
    this.assumeDatePartitioning = assumeDatePartitioning;
    this.fs = fs;
    this.activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public abstract void createTable(String tableName, MessageType storageSchema,
                                   String inputFormatClass, String outputFormatClass, String serdeClass);

  public abstract boolean doesTableExist(String tableName);

  public abstract Option<String> getLastCommitTimeSynced(String tableName);

  public abstract void updateLastCommitTimeSynced(String tableName);

  public abstract void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  public abstract void updatePartitionsToTable(String tableName, List<String> changedPartitions);

  public HoodieTimeline getActiveTimeline() {
    return activeTimeline;
  }

  public HoodieTableType getTableType() {
    return tableType;
  }

  public String getBasePath() {
    return metaClient.getBasePath();
  }

  public FileSystem getFs() {
    return fs;
  }

  public void closeQuietly(ResultSet resultSet, Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close the statement opened ", e);
    }

    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close the resultset opened ", e);
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

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public List<String> getPartitionsWrittenToSince(Option<String> lastCommitTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      LOG.info("Last commit time synced is not known, listing all partitions in " + basePath + ",FS :" + fs);
      try {
        return FSUtils.getAllPartitionPaths(fs, basePath, assumeDatePartitioning);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to list all partitions in " + basePath, e);
      }
    } else {
      LOG.info("Last commit time synced is " + lastCommitTimeSynced.get() + ", Getting commits since then");

      HoodieTimeline timelineToSync = activeTimeline.findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE);
      return timelineToSync.getInstants().map(s -> {
        try {
          return HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(s).get(), HoodieCommitMetadata.class);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to get partitions written since " + lastCommitTimeSynced, e);
        }
      }).flatMap(s -> s.getPartitionToWriteStats().keySet().stream()).distinct().collect(Collectors.toList());
    }
  }

  private MessageType readSchemaFromLastCompaction(Option<HoodieInstant> lastCompactionCommitOpt) throws IOException {
    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(() -> new HoodieSyncException(
            "Could not read schema from last compaction, no compaction commits found on path " + basePath));

    // Read from the compacted file wrote
    HoodieCommitMetadata compactionMetadata = HoodieCommitMetadata
            .fromBytes(activeTimeline.getInstantDetails(lastCompactionCommit).get(), HoodieCommitMetadata.class);
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
            .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for compaction "
                    + lastCompactionCommit + ", could not get schema for table " + metaClient.getBasePath()));
    return readSchemaFromBaseFile(new Path(filePath));
  }

  /**
   * Read the parquet schema from a parquet File.
   */
  private MessageType readSchemaFromBaseFile(Path parquetFilePath) throws IOException {
    LOG.info("Reading schema from " + parquetFilePath);
    if (!fs.exists(parquetFilePath)) {
      throw new IllegalArgumentException(
              "Failed to read schema from data file " + parquetFilePath + ". File does not exist.");
    }
    ParquetMetadata fileFooter =
            ParquetFileReader.readFooter(fs.getConf(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    return fileFooter.getFileMetaData().getSchema();
  }

  /**
   * Read the schema from the log file on path.
  */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLogFile(Option<HoodieInstant> lastCompactionCommitOpt, Path path) throws IOException {
    MessageType messageType = TableSchemaResolver.readSchemaFromLogFile(fs, path);
    // Fall back to read the schema from last compaction
    if (messageType == null) {
      LOG.info("Falling back to read the schema from last compaction " + lastCompactionCommitOpt);
      return readSchemaFromLastCompaction(lastCompactionCommitOpt);
    }
    return messageType;
  }

  /**
   * Partition Event captures any partition that needs to be added or updated.
   */
  public static class PartitionEvent {

    public enum PartitionEventType {
      ADD, UPDATE
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
  }
}
