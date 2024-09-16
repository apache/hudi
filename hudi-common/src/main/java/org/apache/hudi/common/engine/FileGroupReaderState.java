/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.engine;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

public class FileGroupReaderState {
  private HoodieFileGroupReaderSchemaHandler schemaHandler = null;
  private String latestCommitTime = null;
  private HoodieRecordMerger recordMerger = null;
  private RecordMergeMode recordMergeMode = null;
  private Boolean hasLogFiles = null;
  private Boolean hasBootstrapBaseFile = null;
  private Boolean needsBootstrapMerge = null;
  private Boolean shouldMergeUseRecordPosition = null;
  private Boolean supportsParquetRowIndex = null;
  private HoodieTableMetaClient metaClient = null;
  private Option<String> partitionNameOverrideOpt = null;
  private Option<String[]> partitionPathFieldOpt = null;
  private TypedProperties props = null;
  private Integer bufferSize = null;

  // Getter and Setter for schemaHandler
  public HoodieFileGroupReaderSchemaHandler getSchemaHandler() {
    return schemaHandler;
  }

  public void setSchemaHandler(HoodieFileGroupReaderSchemaHandler schemaHandler) {
    this.schemaHandler = schemaHandler;
  }

  public Schema getDataSchema() {
    return this.schemaHandler.getDataSchema();
  }

  public Schema getRequestedSchema() {
    return this.schemaHandler.getRequestedSchema();
  }

  public Schema getRequiredSchema() {
    return this.schemaHandler.getRequiredSchema();
  }

  public Option<InternalSchema> getInternalSchemaOpt() {
    return this.schemaHandler.getInternalSchemaOpt();
  }

  public void setPartitionNameOverrideOpt(Option<String> partitionNameOverrideOpt) {
    this.partitionNameOverrideOpt = partitionNameOverrideOpt;
  }

  public Option<String> getPartitionNameOverrideOpt() {
    return partitionNameOverrideOpt;
  }

  public void setPartitionPathFieldOpt(Option<String[]> partitionPathFieldOpt) {
    this.partitionPathFieldOpt = partitionPathFieldOpt;
  }

  public Option<String[]> getPartitionPathFieldOpt() {
    return partitionPathFieldOpt;
  }

  public void setProps(TypedProperties props) {
    this.props = props;
  }

  public TypedProperties getProps() {
    return props;
  }

  public int getBufferSize() {
    if (this.bufferSize == null) {
      this.bufferSize = ConfigUtils.getIntWithAltKeys(getProps(), HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE);
    }
    return this.bufferSize;
  }

  public InternalSchema getInternalSchema() {
    return this.schemaHandler.getInternalSchema();
  }

  public String getLatestCommitTime() {
    return latestCommitTime;
  }

  public void setLatestCommitTime(String latestCommitTime) {
    this.latestCommitTime = latestCommitTime;
  }

  public HoodieRecordMerger getRecordMerger() {
    return recordMerger;
  }

  public void setRecordMerger(HoodieRecordMerger recordMerger) {
    this.recordMerger = recordMerger;
  }

  public RecordMergeMode getRecordMergeMode() {
    if (this.recordMergeMode == null) {
      this.recordMergeMode = RecordMergeMode.valueOf(getStringWithAltKeys(props, HoodieCommonConfig.RECORD_MERGE_MODE, true).toUpperCase());
    }
    return recordMergeMode;
  }

  // Getter and Setter for hasLogFiles
  public boolean getHasLogFiles() {
    return hasLogFiles;
  }

  public void setHasLogFiles(boolean hasLogFiles) {
    this.hasLogFiles = hasLogFiles;
  }

  // Getter and Setter for hasBootstrapBaseFile
  public boolean getHasBootstrapBaseFile() {
    return hasBootstrapBaseFile;
  }

  public void setHasBootstrapBaseFile(boolean hasBootstrapBaseFile) {
    this.hasBootstrapBaseFile = hasBootstrapBaseFile;
  }

  // Getter and Setter for needsBootstrapMerge
  public boolean getNeedsBootstrapMerge() {
    return needsBootstrapMerge;
  }

  public void setNeedsBootstrapMerge(boolean needsBootstrapMerge) {
    this.needsBootstrapMerge = needsBootstrapMerge;
  }

  // Getter and Setter for useRecordPosition
  public boolean getShouldMergeUseRecordPosition() {
    return shouldMergeUseRecordPosition;
  }

  public void setShouldMergeUseRecordPosition(boolean shouldMergeUseRecordPosition) {
    this.shouldMergeUseRecordPosition = shouldMergeUseRecordPosition;
  }

  public void setSupportsParquetRowIndex(boolean supportsParquetRowIndex) {
    this.supportsParquetRowIndex = supportsParquetRowIndex;
  }

  public boolean getSupportsParquetRowIndex() {
    return supportsParquetRowIndex;
  }

  public void setMetaClient(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }
}
