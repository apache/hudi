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

package org.apache.hudi.table.format.cdc;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * The base InputFormat class to read Hoodie data set as change logs.
 */
public class CdcInputFormat extends MergeOnReadInputFormat {
  private static final long serialVersionUID = 1L;

  private CdcInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      List<Predicate> predicates,
      long limit,
      boolean emitDelete) {
    super(conf, tableState, fieldTypes, predicates, limit, emitDelete, InternalSchemaManager.DISABLED);
  }

  @Override
  protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
    if (split instanceof CdcInputSplit) {
      HoodieCDCSupplementalLoggingMode mode = OptionsResolver.getCDCSupplementalLoggingMode(conf);
      CdcImageManager manager = new CdcImageManager(
          tableState.getRowType(),
          FlinkWriteClients.getHoodieClientConfig(conf),
          this::getFileSliceIterator);
      Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc =
          cdcFileSplit -> getRecordIteratorSafe(split.getTablePath(), split.getMaxCompactionMemoryInBytes(), cdcFileSplit, mode, manager);
      return new CdcIterators.CdcFileSplitsIterator(((CdcInputSplit) split).getChanges(), manager, recordIteratorFunc);
    } else {
      return super.initIterator(split);
    }
  }

  /**
   * Returns the builder for {@link CdcInputFormat}.
   */
  public static Builder builder() {
    return new Builder();
  }

  private ClosableIterator<RowData> getFileSliceIterator(MergeOnReadInputSplit split) {
    try {
      final HoodieSchema schema = HoodieSchemaCache.intern(
          HoodieSchema.parse(tableState.getTableSchema()));
      // before/after images use snapshot scan semantics, so emitDelete is false
      return getSplitRowIterator(split, schema, schema, FlinkOptions.REALTIME_PAYLOAD_COMBINE, false);
    } catch (IOException e) {
      throw new HoodieException("Failed to create iterator for split: " + split, e);
    }
  }

  private ClosableIterator<RowData> getRecordIteratorSafe(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      CdcImageManager imageManager) {
    try {
      return getRecordIterator(tablePath, maxCompactionMemoryInBytes, fileSplit, mode, imageManager);
    } catch (IOException e) {
      throw new HoodieException("Get record iterator error", e);
    }
  }

  private ClosableIterator<RowData> getRecordIterator(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      CdcImageManager imageManager) throws IOException {
    switch (fileSplit.getCdcInferCase()) {
      case BASE_FILE_INSERT:
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton");
        String path = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        return new CdcIterators.AddBaseFileIterator(getBaseFileIterator(path));
      case BASE_FILE_DELETE:
        ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
            "Before file slice should exist");
        FileSlice fileSlice = fileSplit.getBeforeFileSlice().get();
        MergeOnReadInputSplit inputSplit = CdcIterators.fileSlice2Split(tablePath, fileSlice, maxCompactionMemoryInBytes);
        return new CdcIterators.RemoveBaseFileIterator(
            tableState.getRequiredRowType(), tableState.getRequiredPositions(), getFileSliceIterator(inputSplit));
      case AS_IS: {
        HoodieSchema tblSchema = HoodieSchema.parse(tableState.getTableSchema());
        HoodieSchema reqSchema = HoodieSchema.parse(tableState.getRequiredSchema());
        HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(tblSchema);
        HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema);
        switch (mode) {
          case DATA_BEFORE_AFTER:
            return new CdcIterators.BeforeAfterImageIterator(
                hadoopConf, tablePath, tblSchema, reqSchema, tableState.getRequiredRowType(), cdcSchema, fileSplit);
          case DATA_BEFORE:
            return new CdcIterators.BeforeImageIterator(
                hadoopConf, tablePath, tblSchema, reqSchema, tableState.getRequiredRowType(),
                tableState.getRequiredPositions(), maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          case OP_KEY_ONLY:
            return new CdcIterators.RecordKeyImageIterator(
                hadoopConf, tablePath, tblSchema, reqSchema, tableState.getRequiredRowType(),
                tableState.getRequiredPositions(), maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          default:
            throw new AssertionError("Unexpected mode: " + mode);
        }
      }
      case LOG_FILE:
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton");
        String logFilepath = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        MergeOnReadInputSplit split = CdcIterators.singleLogFile2Split(tablePath, logFilepath, maxCompactionMemoryInBytes);
        ClosableIterator<HoodieRecord<RowData>> recordIterator = getSplitRecordIterator(split);
        return new CdcIterators.DataLogFileIterator(
            maxCompactionMemoryInBytes, imageManager, fileSplit,
            HoodieSchema.parse(tableState.getTableSchema()),
            tableState.getRequiredRowType(), tableState.getRequiredPositions(),
            recordIterator, metaClient, imageManager.getWriteConfig());
      case REPLACE_COMMIT:
        return new CdcIterators.ReplaceCommitIterator(
            tablePath, tableState.getRequiredRowType(), tableState.getRequiredPositions(),
            maxCompactionMemoryInBytes, fileSplit, this::getFileSliceIterator);
      default:
        throw new AssertionError("Unexpected cdc file split infer case: " + fileSplit.getCdcInferCase());
    }
  }

  /**
   * Get a {@link HoodieRecord} iterator using {@link HoodieFileGroupReader}.
   *
   * @param split input split
   *
   * @return {@link RowData} iterator for the given split.
   */
  private ClosableIterator<HoodieRecord<RowData>> getSplitRecordIterator(MergeOnReadInputSplit split) throws IOException {
    final HoodieSchema schema = HoodieSchemaCache.intern(
        HoodieSchema.parse(tableState.getTableSchema()));
    HoodieFileGroupReader<RowData> fileGroupReader =
        createFileGroupReader(split, schema, schema, FlinkOptions.REALTIME_PAYLOAD_COMBINE, true);
    return fileGroupReader.getClosableHoodieRecordIterator();
  }

  /**
   * Builder for {@link CdcInputFormat}.
   */
  public static class Builder extends MergeOnReadInputFormat.Builder {

    public Builder config(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder tableState(MergeOnReadTableState tableState) {
      this.tableState = tableState;
      return this;
    }

    public Builder fieldTypes(List<DataType> fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder predicates(List<Predicate> predicates) {
      this.predicates = predicates;
      return this;
    }

    public Builder limit(long limit) {
      this.limit = limit;
      return this;
    }

    public Builder emitDelete(boolean emitDelete) {
      this.emitDelete = emitDelete;
      return this;
    }

    public CdcInputFormat build() {
      return new CdcInputFormat(conf, tableState, fieldTypes, predicates, limit, emitDelete);
    }
  }
}
