/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkWriteClients;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default reader function implementation for both MOR and COW tables.
 */
public class HoodieSplitReaderFunction implements SplitReaderFunction<RowData> {
  private final HoodieTableMetaClient metaClient;
  private final HoodieSchema tableSchema;
  private final HoodieSchema requiredSchema;
  private final Configuration configuration;
  private final HoodieWriteConfig writeConfig;
  private final String mergeType;
  private final boolean emitDelete;
  private final List<ExpressionPredicates.Predicate> predicates;
  private HoodieFileGroupReader<RowData> fileGroupReader;

  public HoodieSplitReaderFunction(
      HoodieTableMetaClient metaClient,
      Configuration configuration,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String mergeType,
      List<ExpressionPredicates.Predicate> predicates,
      boolean emitDelete) {

    ValidationUtils.checkArgument(tableSchema != null, "tableSchema can't be null");
    ValidationUtils.checkArgument(requiredSchema != null, "requiredSchema can't be null");
    this.metaClient = metaClient;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.configuration = configuration;
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(configuration);
    this.predicates = predicates;
    this.mergeType = mergeType;
    this.emitDelete = emitDelete;
    this.fileGroupReader = null;
  }

  @Override
  public RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> read(HoodieSourceSplit split) {
    final String splitId = split.splitId();
    try {
      this.fileGroupReader = createFileGroupReader(split);
      final ClosableIterator<RowData> recordIterator = fileGroupReader.getClosableIterator();
      BatchRecords<RowData> records = BatchRecords.forRecords(splitId, recordIterator, split.getFileOffset(), split.getConsumed());
      records.seek(split.getConsumed());
      return records;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from file group: " + split.getFileId(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (fileGroupReader != null) {
      fileGroupReader.close();
    }
  }

  /**
   * Creates a {@link HoodieFileGroupReader} for the given split.
   *
   * @param split The source split to read
   * @return A {@link HoodieFileGroupReader} instance
   */
  private HoodieFileGroupReader<RowData> createFileGroupReader(HoodieSourceSplit split) {
    // Create FileSlice from split information
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(split.getPartitionPath(), split.getFileId()),
        "",
        split.getBasePath().map(HoodieBaseFile::new).orElse(null),
        split.getLogPaths().map(logFiles ->
            logFiles.stream().map(HoodieLogFile::new).collect(Collectors.toList())
        ).orElse(Collections.emptyList())
    );

    return FormatUtils.createFileGroupReader(
      metaClient,
      writeConfig,
      InternalSchemaManager.get(metaClient.getStorageConf(), metaClient),
      fileSlice,
      tableSchema,
      requiredSchema,
      split.getLatestCommit(),
      mergeType,
      emitDelete,
      predicates,
      split.getInstantRange()
    );
  }
}
