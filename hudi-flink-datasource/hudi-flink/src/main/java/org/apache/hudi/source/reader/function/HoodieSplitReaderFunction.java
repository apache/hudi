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

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.reader.DefaultHoodieBatchReader;
import org.apache.hudi.source.reader.RowDataRecordCloner;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.hudi.table.format.FlinkReaderContextFactory;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Default reader function implementation for both MOR and COW tables.
 */
public class HoodieSplitReaderFunction implements SplitReaderFunction<RowData> {
  private final HoodieTableMetaClient metaClient;
  private final HoodieSchema tableSchema;
  private final HoodieSchema requiredSchema;
  private final Configuration configuration;
  private final Option<InternalSchema> internalSchemaOption;
  private final String mergeType;
  private HoodieFileGroupReader<RowData> fileGroupReader;

  public HoodieSplitReaderFunction(
      HoodieTableMetaClient metaClient,
      Configuration configuration,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String mergeType,
      Option<InternalSchema> internalSchemaOption) {

    ValidationUtils.checkArgument(tableSchema != null, "tableSchema can't be null");
    ValidationUtils.checkArgument(requiredSchema != null, "requiredSchema can't be null");
    this.metaClient = metaClient;
    this.tableSchema = tableSchema;
    this.configuration = configuration;
    this.requiredSchema = requiredSchema;
    this.internalSchemaOption = internalSchemaOption;
    this.mergeType = mergeType;
    this.fileGroupReader = null;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> read(HoodieSourceSplit split) {
    try {
      this.fileGroupReader = createFileGroupReader(split);
      final ClosableIterator<RowData> recordIterator = fileGroupReader.getClosableIterator();
      final RowDataRecordCloner recordCloner = new RowDataRecordCloner(HoodieSchemaConverter.convertToRowType(requiredSchema));
      DefaultHoodieBatchReader<RowData> defaultBatchReader = new DefaultHoodieBatchReader<RowData>(configuration, recordCloner);
      return defaultBatchReader.batch(split, recordIterator);
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

    FlinkReaderContextFactory readerContextFactory = new FlinkReaderContextFactory(metaClient);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(configuration);

    final TypedProperties typedProps = FlinkClientUtil.getReadProps(metaClient.getTableConfig(), writeConfig);
    typedProps.put(HoodieReaderConfig.MERGE_TYPE.key(), mergeType);

    // Build the file group reader
    HoodieFileGroupReader.Builder<RowData> builder = HoodieFileGroupReader.<RowData>newBuilder()
            .withReaderContext(readerContextFactory.getContext())
            .withHoodieTableMetaClient(metaClient)
            .withFileSlice(fileSlice)
            .withProps(typedProps)
            .withShouldUseRecordPosition(true)
            .withDataSchema(tableSchema)
            .withLatestCommitTime(split.getLatestCommit())
            .withRequestedSchema(requiredSchema);

    if (internalSchemaOption.isPresent()) {
      builder.withInternalSchema(internalSchemaOption);
    }

    return builder.build();
  }
}
