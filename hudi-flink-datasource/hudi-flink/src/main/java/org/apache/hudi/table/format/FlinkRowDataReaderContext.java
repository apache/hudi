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

package org.apache.hudi.table.format;

import org.apache.hudi.client.model.BootstrapRowData;
import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RecordKeyToRowDataConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

/**
 * Implementation of {@link HoodieReaderContext} to read {@link RowData}s from base files or
 * log files with Flink parquet reader.
 */
public class FlinkRowDataReaderContext extends HoodieReaderContext<RowData> {
  private final List<ExpressionPredicates.Predicate> predicates;
  private final Supplier<InternalSchemaManager> internalSchemaManager;
  private final HoodieTableConfig tableConfig;

  public FlinkRowDataReaderContext(
      StorageConfiguration<?> storageConfiguration,
      Supplier<InternalSchemaManager> internalSchemaManager,
      List<ExpressionPredicates.Predicate> predicates,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt) {
    super(storageConfiguration, tableConfig, instantRangeOpt, Option.empty(), new FlinkRecordContext(tableConfig, storageConfiguration));
    this.tableConfig = tableConfig;
    this.internalSchemaManager = internalSchemaManager;
    this.predicates = predicates;
  }

  @Override
  public ClosableIterator<RowData> getFileRecordIterator(
      StoragePath filePath,
      long start,
      long length,
      Schema dataSchema,
      Schema requiredSchema,
      HoodieStorage storage) throws IOException {
    boolean isLogFile = FSUtils.isLogFile(filePath);
    // disable schema evolution in fileReader if it's log file, since schema evolution for log file is handled in `FileGroupRecordBuffer`
    InternalSchemaManager schemaManager = isLogFile ? InternalSchemaManager.DISABLED : internalSchemaManager.get();

    HoodieRowDataParquetReader rowDataParquetReader =
        (HoodieRowDataParquetReader) HoodieIOFactory.getIOFactory(storage)
            .getReaderFactory(HoodieRecord.HoodieRecordType.FLINK)
            .getFileReader(tableConfig, filePath, HoodieFileFormat.PARQUET, Option.empty());
    DataType rowType = RowDataAvroQueryContexts.fromAvroSchema(dataSchema).getRowType();
    return rowDataParquetReader.getRowDataIterator(schemaManager, rowType, requiredSchema, predicates);
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new EventTimeFlinkRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new CommitTimeFlinkRecordMerger());
      default:
        if (mergeImplClasses.contains(PartialUpdateFlinkRecordMerger.class.getName())) {
          return Option.of(new PartialUpdateFlinkRecordMerger());
        }
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.FLINK, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new HoodieValidationException("No valid flink merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public ClosableIterator<RowData> mergeBootstrapReaders(
      ClosableIterator<RowData> skeletonFileIterator,
      Schema skeletonRequiredSchema,
      ClosableIterator<RowData> dataFileIterator,
      Schema dataRequiredSchema,
      List<Pair<String, Object>> partitionFieldAndValues) {
    Map<Integer, Object> partitionOrdinalToValues = partitionFieldAndValues.stream()
        .collect(Collectors.toMap(
            pair -> dataRequiredSchema.getField(pair.getKey()).pos(),
            Pair::getValue));
    return new ClosableIterator<RowData>() {
      final JoinedRowData joinedRow = new JoinedRowData();
      @Override
      public boolean hasNext() {
        boolean skeletonHasNext = skeletonFileIterator.hasNext();
        boolean dataHasNext = dataFileIterator.hasNext();
        ValidationUtils.checkArgument(skeletonHasNext == dataHasNext,
            "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!");
        return skeletonHasNext;
      }

      @Override
      public RowData next() {
        RowData skeletonRow = skeletonFileIterator.next();
        RowData dataRow = appendPartitionFields(dataFileIterator.next());
        joinedRow.setRowKind(dataRow.getRowKind());
        joinedRow.replace(skeletonRow, dataRow);
        return joinedRow;
      }

      private RowData appendPartitionFields(RowData dataRow) {
        if (partitionFieldAndValues.isEmpty()) {
          return dataRow;
        }
        return new BootstrapRowData(dataRow, partitionOrdinalToValues);
      }

      @Override
      public void close() {
        skeletonFileIterator.close();
        dataFileIterator.close();
      }
    };
  }

  @Override
  public void setSchemaHandler(FileGroupReaderSchemaHandler<RowData> schemaHandler) {
    super.setSchemaHandler(schemaHandler);
    // init ordering value converter: java -> engine type
    List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(getMergeMode(), tableConfig);
    ((FlinkRecordContext) recordContext).initOrderingValueConverter(schemaHandler.getTableSchema(), orderingFieldNames);

    Option<String[]> recordKeysOpt = tableConfig.getRecordKeyFields();
    if (recordKeysOpt.isEmpty()) {
      return;
    }
    // primary key semantic is lost if not all primary key fields are included in the request schema.
    boolean pkSemanticLost = Arrays.stream(recordKeysOpt.get()).anyMatch(k -> schemaHandler.getRequestedSchema().getField(k) == null);
    if (pkSemanticLost) {
      return;
    }
    Schema requiredSchema = schemaHandler.getRequiredSchema();
    // get primary key field position in required schema.
    int[] pkFieldsPos = Arrays.stream(recordKeysOpt.get())
        .map(k -> Option.ofNullable(requiredSchema.getField(k)).map(Schema.Field::pos).orElse(-1))
        .mapToInt(Integer::intValue)
        .toArray();
    // the converter is used to create a RowData contains primary key fields only
    // for DELETE cases, it'll not be initialized if primary key semantics is lost.
    // For e.g, if the pk fields are [a, b] but user only select a, then the pk
    // semantics is lost.
    RecordKeyToRowDataConverter recordKeyRowConverter = new RecordKeyToRowDataConverter(
        pkFieldsPos, (RowType) RowDataAvroQueryContexts.fromAvroSchema(requiredSchema).getRowType().getLogicalType());
    ((FlinkRecordContext) recordContext).setRecordKeyRowConverter(recordKeyRowConverter);
  }
}
