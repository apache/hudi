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

import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.HoodieFlinkRecord;
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
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.row.RowDataFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.expression.Predicate;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkRowProjection;
import org.apache.hudi.util.RowDataUtil;
import org.apache.hudi.util.RowDataUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

/**
 * Implementation of {@link HoodieReaderContext} to read {@link RowData}s from base files or
 * log files with Flink parquet reader.
 */
public class FlinkRowDataReaderContext extends HoodieReaderContext<RowData> {
  private final List<Predicate> predicates;
  private final InternalSchemaManager internalSchemaManager;
  private RowDataSerializer rowDataSerializer;
  private final Configuration conf;

  public FlinkRowDataReaderContext(
      Configuration conf,
      InternalSchemaManager internalSchemaManager,
      List<Predicate> predicates) {
    this.conf = conf;
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
    InternalSchemaManager schemaManager = isLogFile ? InternalSchemaManager.DISABLED : internalSchemaManager;
    RowDataFileReaderFactories.Factory readerFactory = RowDataFileReaderFactories.getFactory(HoodieFileFormat.PARQUET);
    RowDataFileReader fileReader = readerFactory.createFileReader(schemaManager, conf);

    List<String> fieldNames = dataSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    List<DataType> fieldTypes = dataSchema.getFields().stream().map(
        f -> AvroSchemaConverter.convertToDataType(f.schema())).collect(Collectors.toList());
    int[] selectedFields = requiredSchema.getFields().stream().map(Schema.Field::name)
        .map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();

    return fileReader.getRowDataIterator(
        fieldNames, fieldTypes, selectedFields, predicates, filePath, start, length);
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new EventTimeFlinkRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new CommitTimeFlinkRecordMerger());
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
    RowData.FieldGetter fieldGetter = RowDataUtil.internFieldGetter(schema, fieldName);
    return fieldGetter.getFieldOrNull(record);
  }

  @Override
  public String getRecordKey(RowData record, Schema schema) {
    return Objects.toString(getValue(record, schema, RECORD_KEY_METADATA_FIELD));
  }

  @Override
  public HoodieRecord<RowData> constructHoodieRecord(Option<RowData> recordOption, Map<String, Object> metadataMap) {
    HoodieKey hoodieKey = new HoodieKey(
        (String) metadataMap.get(INTERNAL_META_RECORD_KEY),
        (String) metadataMap.get(INTERNAL_META_PARTITION_PATH));
    RowData rowData = recordOption.get();
    // delete record
    if (recordOption.isEmpty()) {
      Comparable orderingValue;
      if (metadataMap.containsKey(INTERNAL_META_ORDERING_FIELD)) {
        orderingValue = (Comparable) metadataMap.get(INTERNAL_META_ORDERING_FIELD);
      } else {
        throw new HoodieException("There should be ordering value in metadataMap.");
      }
      return new HoodieEmptyRecord<>(hoodieKey, HoodieOperation.DELETE, orderingValue, HoodieRecord.HoodieRecordType.FLINK);
    }
    return new HoodieFlinkRecord(hoodieKey, rowData);
  }

  @Override
  public Comparable getOrderingValue(Option<RowData> recordOption, Map<String, Object> metadataMap, Schema schema, Option<String> orderingFieldName) {
    if (metadataMap.containsKey(INTERNAL_META_ORDERING_FIELD)) {
      return (Comparable) metadataMap.get(INTERNAL_META_ORDERING_FIELD);
    }
    if (!recordOption.isPresent() || orderingFieldName.isEmpty()) {
      return DEFAULT_ORDERING_VALUE;
    }
    Object value = getValue(recordOption.get(), schema, orderingFieldName.get());
    // currently the ordering value stored in DeleteRecord is converted to Avro value because Flink reader currently uses AVRO payload to merge.
    // So here we align the data format with reader until RowData reader is supported, HUDI-9146.
    UnaryOperator<Object> fieldConverter = RowDataUtil.internFieldConverter(schema, orderingFieldName.get(), conf.get(FlinkOptions.READ_UTC_TIMEZONE));
    Comparable finalOrderingVal = value != null ? (Comparable) fieldConverter.apply(value) : DEFAULT_ORDERING_VALUE;
    metadataMap.put(INTERNAL_META_ORDERING_FIELD, finalOrderingVal);
    return finalOrderingVal;
  }

  @Override
  public RowData seal(RowData rowData) {
    if (rowDataSerializer == null) {
      RowType requiredRowType = (RowType) AvroSchemaConverter.convertToDataType(getSchemaHandler().getRequiredSchema()).getLogicalType();
      rowDataSerializer = new RowDataSerializer(requiredRowType);
    }
    // copy is unnecessary if there is no caching in subsequent processing.
    return rowDataSerializer.copy(rowData);
  }

  @Override
  public ClosableIterator<RowData> mergeBootstrapReaders(
      ClosableIterator<RowData> skeletonFileIterator,
      Schema skeletonRequiredSchema,
      ClosableIterator<RowData> dataFileIterator,
      Schema dataRequiredSchema) {
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
        RowData dataRow = dataFileIterator.next();
        joinedRow.setRowKind(dataRow.getRowKind());
        joinedRow.replace(skeletonRow, dataRow);
        return joinedRow;
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
    FlinkRowProjection rowProjection = RowDataUtils.internRowDataProjection(from, to, renamedColumns);
    return rowProjection::project;
  }

  @Override
  public GenericRecord convertToAvroRecord(RowData record, Schema schema) {
    throw new UnsupportedOperationException("FlinkRowDataReaderContext do not support convertToAvroRecord yet.");
  }

  @Override
  public RowData convertAvroRecord(IndexedRecord avroRecord) {
    throw new UnsupportedOperationException("FlinkRowDataReaderContext do not support convertAvroRecord yet.");
  }
}
