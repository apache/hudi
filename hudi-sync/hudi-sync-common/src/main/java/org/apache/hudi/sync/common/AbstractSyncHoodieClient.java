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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractSyncHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractSyncHoodieClient.class);

  public static final TypeConverter TYPE_CONVERTOR = new TypeConverter() {};

  protected final HoodieTableMetaClient metaClient;
  protected final HoodieTableType tableType;
  protected final FileSystem fs;
  private final String basePath;
  private final boolean assumeDatePartitioning;
  private final boolean useFileListingFromMetadata;
  private final boolean withOperationField;

  @Deprecated
  public AbstractSyncHoodieClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata,
                                  boolean verifyMetadataFileListing, boolean withOperationField, FileSystem fs) {
    this(basePath, assumeDatePartitioning, useFileListingFromMetadata, withOperationField, fs);
  }

  public AbstractSyncHoodieClient(String basePath, boolean assumeDatePartitioning, boolean useFileListingFromMetadata,
                                  boolean withOperationField, FileSystem fs) {
    this.metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    this.tableType = metaClient.getTableType();
    this.basePath = basePath;
    this.assumeDatePartitioning = assumeDatePartitioning;
    this.useFileListingFromMetadata = useFileListingFromMetadata;
    this.withOperationField = withOperationField;
    this.fs = fs;
  }

  /**
   * Create the table.
   * @param tableName The table name.
   * @param storageSchema The table schema.
   * @param inputFormatClass The input format class of this table.
   * @param outputFormatClass The output format class of this table.
   * @param serdeClass The serde class of this table.
   * @param serdeProperties The serde properties of this table.
   * @param tableProperties The table properties for this table.
   */
  public abstract void createTable(String tableName, MessageType storageSchema,
                                   String inputFormatClass, String outputFormatClass,
                                   String serdeClass, Map<String, String> serdeProperties,
                                   Map<String, String> tableProperties);

  public abstract boolean doesTableExist(String tableName);

  public abstract Option<String> getLastCommitTimeSynced(String tableName);

  public abstract void updateLastCommitTimeSynced(String tableName);

  public abstract void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  public abstract void updatePartitionsToTable(String tableName, List<String> changedPartitions);

  public  void updateTableProperties(String tableName, Map<String, String> tableProperties) {}

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
      if (withOperationField) {
        return new TableSchemaResolver(metaClient, true).getTableParquetSchema();
      } else {
        return new TableSchemaResolver(metaClient).getTableParquetSchema();
      }
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read data schema", e);
    }
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

  public abstract static class TypeConverter implements Serializable {

    static final String DEFAULT_TARGET_TYPE = "DECIMAL";

    protected String targetType;

    public TypeConverter() {
      this.targetType = DEFAULT_TARGET_TYPE;
    }

    public TypeConverter(String targetType) {
      ValidationUtils.checkArgument(Objects.nonNull(targetType));
      this.targetType = targetType;
    }

    public void doConvert(ResultSet resultSet, Map<String, String> schema) throws SQLException {
      schema.put(getColumnName(resultSet), targetType.equalsIgnoreCase(getColumnType(resultSet))
                ? convert(resultSet) : getColumnType(resultSet));
    }

    public String convert(ResultSet resultSet) throws SQLException {
      String columnType = getColumnType(resultSet);
      int columnSize = resultSet.getInt("COLUMN_SIZE");
      int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
      return columnType + String.format("(%s,%s)", columnSize, decimalDigits);
    }

    public String getColumnName(ResultSet resultSet) throws SQLException {
      return resultSet.getString(4);
    }

    public String getColumnType(ResultSet resultSet) throws SQLException {
      return resultSet.getString(6);
    }
  }

  /**
   * Read the schema from the log file on path.
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLogFile(Option<HoodieInstant> lastCompactionCommitOpt, Path path) throws Exception {
    MessageType messageType = TableSchemaResolver.readSchemaFromLogFile(fs, path);
    // Fall back to read the schema from last compaction
    if (messageType == null) {
      LOG.info("Falling back to read the schema from last compaction " + lastCompactionCommitOpt);
      return new TableSchemaResolver(this.metaClient).readSchemaFromLastCompaction(lastCompactionCommitOpt);
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
