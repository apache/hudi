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
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RowProjection;
import org.apache.hudi.util.SchemaEvolvingRowDataProjection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;

/**
 * Implementation of {@link HoodieReaderContext} to read {@link RowData}s from base files or
 * log files with Flink parquet reader.
 */
public class FlinkRowDataReaderContext extends HoodieReaderContext<RowData> {
  private final List<Predicate> predicates;
  private final Supplier<InternalSchemaManager> internalSchemaManager;
  private final boolean utcTimezone;
  private final HoodieConfig hoodieConfig;

  public FlinkRowDataReaderContext(
      StorageConfiguration<?> storageConfiguration,
      Supplier<InternalSchemaManager> internalSchemaManager,
      List<Predicate> predicates,
      HoodieTableConfig tableConfig) {
    super(storageConfiguration, tableConfig);
    this.hoodieConfig = tableConfig;
    this.internalSchemaManager = internalSchemaManager;
    this.predicates = predicates;
    this.utcTimezone = getStorageConfiguration().getBoolean(FlinkOptions.READ_UTC_TIMEZONE.key(), FlinkOptions.READ_UTC_TIMEZONE.defaultValue());
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
            .getFileReader(hoodieConfig, filePath, HoodieFileFormat.PARQUET, Option.empty());
    return rowDataParquetReader.getRowDataIterator(schemaManager, requiredSchema);
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(EventTimeFlinkRecordMerger.INSTANCE);
      case COMMIT_TIME_ORDERING:
        return Option.of(CommitTimeFlinkRecordMerger.INSTANCE);
      default:
        Option<HoodieRecordMerger> mergerClass =
            HoodieRecordUtils.createValidRecordMerger(EngineType.FLINK, mergeImplClasses, mergeStrategyId);
        if (mergerClass.isEmpty()) {
          throw new HoodieValidationException("No valid flink merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return mergerClass;
    }
  }

  @Override
  public Object getValue(RowData record, Schema schema, String fieldName) {
    RowDataAvroQueryContexts.FieldQueryContext fieldQueryContext =
        RowDataAvroQueryContexts.fromAvroSchema(schema, utcTimezone).getFieldQueryContext(fieldName);
    if (fieldQueryContext == null) {
      return null;
    } else {
      return fieldQueryContext.getFieldGetter().getFieldOrNull(record);
    }
  }

  @Override
  public HoodieRecord<RowData> constructHoodieRecord(BufferedRecord<RowData> bufferedRecord) {
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), null);
    // delete record
    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(hoodieKey, HoodieOperation.DELETE, bufferedRecord.getOrderingValue(), HoodieRecord.HoodieRecordType.FLINK);
    }
    RowData rowData = bufferedRecord.getRecord();
    HoodieOperation operation = HoodieOperation.fromValue(rowData.getRowKind().toByteValue());
    return new HoodieFlinkRecord(hoodieKey, operation, bufferedRecord.getOrderingValue(), rowData);
  }

  @Override
  public Comparable getOrderingValue(
      RowData record,
      Schema schema,
      Option<String> orderingFieldName) {
    if (orderingFieldName.isEmpty()) {
      return DEFAULT_ORDERING_VALUE;
    }
    RowDataAvroQueryContexts.FieldQueryContext context = RowDataAvroQueryContexts.fromAvroSchema(schema, utcTimezone).getFieldQueryContext(orderingFieldName.get());
    Comparable finalOrderingVal = (Comparable) context.getValAsJava(record, false);
    return finalOrderingVal;
  }

  @Override
  public RowData seal(RowData rowData) {
    if (rowData instanceof BinaryRowData) {
      return ((BinaryRowData) rowData).copy();
    }
    return rowData;
  }

  @Override
  public RowData toBinaryRow(Schema avroSchema, RowData record) {
    if (record instanceof BinaryRowData) {
      return record;
    }
    RowDataSerializer rowDataSerializer = RowDataAvroQueryContexts.getRowDataSerializer(avroSchema);
    return rowDataSerializer.toBinaryRow(record);
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

  /**
   * Creates a function that will reorder records of schema "from" to schema of "to".
   * It's possible there exist fields in `to` schema, but not in `from` schema because of schema
   * evolution.
   *
   * @param from           the schema of records to be passed into UnaryOperator
   * @param to             the schema of records produced by UnaryOperator
   * @param renamedColumns map of renamed columns where the key is the new name from the query and
   *                       the value is the old name that exists in the file
   * @return a function that takes in a record and returns the record with reordered columns
   */
  @Override
  public UnaryOperator<RowData> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    RowType fromType = (RowType) RowDataAvroQueryContexts.fromAvroSchema(from).getRowType().getLogicalType();
    RowType toType =  (RowType) RowDataAvroQueryContexts.fromAvroSchema(to).getRowType().getLogicalType();
    RowProjection rowProjection = SchemaEvolvingRowDataProjection.instance(fromType, toType, renamedColumns);
    return rowProjection::project;
  }

  @Override
  public GenericRecord convertToAvroRecord(RowData record, Schema schema) {
    throw new UnsupportedOperationException("FlinkRowDataReaderContext do not support convertToAvroRecord yet.");
  }

  @Override
  public RowData convertAvroRecord(IndexedRecord avroRecord) {
    Schema recordSchema = avroRecord.getSchema();
    AvroToRowDataConverters.AvroToRowDataConverter converter = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone).getAvroToRowDataConverter();
    return (RowData) converter.convert(avroRecord);
  }
}
