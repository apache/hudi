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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default reader function implementation for both MOR and COW tables.
 */
public class HoodieSplitReaderFunction extends AbstractSplitReaderFunction {
  private final HoodieSchema tableSchema;
  private final HoodieSchema requiredSchema;
  private final String mergeType;

  public HoodieSplitReaderFunction(
      Configuration configuration,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      InternalSchemaManager internalSchemaManager,
      String mergeType,
      List<ExpressionPredicates.Predicate> predicates,
      boolean emitDelete) {
    super(configuration, predicates, internalSchemaManager, emitDelete);
    ValidationUtils.checkArgument(tableSchema != null, "tableSchema can't be null");
    ValidationUtils.checkArgument(requiredSchema != null, "requiredSchema can't be null");
    ValidationUtils.checkArgument(internalSchemaManager != null, "internalSchemaManager can't be null");
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.mergeType = mergeType;
  }

  @Override
  protected ClosableIterator<RowData> createRecordIterator(HoodieSourceSplit split) {
    HoodieTableMetaClient metaClient = StreamerUtil.metaClientForReader(conf, getHadoopConf());
    // Closing the returned iterator cascade-closes the whole HoodieFileGroupReader, so the base
    // class only has to close the iterator in closeCurrentSplit(). But getClosableIterator() runs
    // initRecordIterators(), which opens the reader's base-file iterator / record buffer before the
    // wrapping iterator is returned; if it throws, the reader is only a local here and nothing else
    // would close it. Keep it in a local and close it in the failure path.
    HoodieRecordReader<RowData> fileGroupReader = createRecordReader(split, metaClient);
    try {
      return fileGroupReader.getClosableIterator();
    } catch (IOException e) {
      closeSuppressing(fileGroupReader, e);
      throw new HoodieIOException("Failed to read from file group: " + split.getFileId(), e);
    } catch (RuntimeException | Error e) {
      closeSuppressing(fileGroupReader, e);
      throw e;
    }
  }

  /** Closes {@code reader}, attaching any close failure to {@code primary} as a suppressed exception. */
  private static void closeSuppressing(HoodieRecordReader<RowData> reader, Throwable primary) {
    try {
      reader.close();
    } catch (Exception closeError) {
      primary.addSuppressed(closeError);
    }
  }

  @Override
  protected RowType producedRowType() {
    return HoodieSchemaConverter.convertToRowType(requiredSchema);
  }

  /**
   * Creates a {@link HoodieRecordReader} for the given split.
   *
   * @param split      The source split to read
   * @param metaClient The table meta client for schema and config resolution
   * @return A {@link HoodieRecordReader} instance
   */
  protected HoodieRecordReader<RowData> createRecordReader(HoodieSourceSplit split, HoodieTableMetaClient metaClient) {
    // Create FileSlice from split information
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(split.getPartitionPath(), split.getFileId()),
        "",
        split.getBasePath().map(HoodieBaseFile::new).orElse(null),
        split.getLogPaths().map(logFiles ->
            logFiles.stream().map(HoodieLogFile::new).collect(Collectors.toList())
        ).orElse(Collections.emptyList())
    );

    return FormatUtils.createRecordReader(
      metaClient,
      getWriteConfig(),
      internalSchemaManager,
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
