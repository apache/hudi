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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Reader function implementation for Merge On Read table.
 */
public class MergeOnReadSplitReaderFunction<I, K, O> implements SplitReaderFunction<RowData> {
  private final HoodieTable<RowData, I, K, O> hoodieTable;
  private final HoodieReaderContext<RowData> readerContext;
  private final HoodieSchema tableSchema;
  private final HoodieSchema requiredSchema;
  private final Option<InternalSchema> internalSchemaOption;
  private final TypedProperties props;
  private HoodieFileGroupReader<RowData> fileGroupReader;

  public MergeOnReadSplitReaderFunction(
      HoodieTable<RowData, I, K, O> hoodieTable,
      HoodieReaderContext<RowData> readerContext,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String mergeType,
      Option<InternalSchema> internalSchemaOption) {

    ValidationUtils.checkArgument(tableSchema != null, "tableSchema can't be null");
    ValidationUtils.checkArgument(requiredSchema != null, "requiredSchema can't be null");

    this.hoodieTable = hoodieTable;
    this.readerContext = readerContext;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.internalSchemaOption = internalSchemaOption;
    this.props = new TypedProperties();
    this.props.put(HoodieReaderConfig.MERGE_TYPE.key(), mergeType);
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

    // Build the file group reader
    HoodieFileGroupReader.Builder<RowData> builder = HoodieFileGroupReader.<RowData>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withFileSlice(fileSlice)
        .withProps(props)
        .withShouldUseRecordPosition(true)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requiredSchema);


    if (internalSchemaOption.isPresent()) {
      builder.withInternalSchema(internalSchemaOption);
    }

    return builder.build();
  }
}
