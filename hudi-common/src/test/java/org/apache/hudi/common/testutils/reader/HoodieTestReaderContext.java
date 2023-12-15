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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroParquetReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;

public class HoodieTestReaderContext extends HoodieReaderContext<IndexedRecord> {
  @Override
  public FileSystem getFs(String path, Configuration conf) {
    return FSUtils.getFs(path, conf);
  }

  @Override
  public ClosableIterator<IndexedRecord> getFileRecordIterator(
      Path filePath,
      long start,
      long length,
      Schema dataSchema,
      Schema requiredSchema,
      Configuration conf
  ) throws IOException {
    HoodieAvroParquetReader reader = new HoodieAvroParquetReader(conf, filePath);
    return reader.getIndexedRecordIterator(dataSchema, requiredSchema);
  }

  @Override
  public IndexedRecord convertAvroRecord(IndexedRecord record) {
    return record;
  }

  @Override
  public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
    switch (mergerStrategy) {
      case DEFAULT_MERGER_STRATEGY_UUID:
        return new HoodieAvroRecordMerger();
      default:
        throw new HoodieException(
            "The merger strategy UUID is not supported: " + mergerStrategy);
    }
  }

  @Override
  public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
    return getFieldValueFromIndexedRecord(record, schema, fieldName);
  }

  @Override
  public String getRecordKey(IndexedRecord record, Schema schema) {
    return getFieldValueFromIndexedRecord(record, schema, ROW_KEY).toString();
  }

  @Override
  public Comparable getOrderingValue(
      Option<IndexedRecord> recordOpt,
      Map<String, Object> metadataMap,
      Schema schema,
      TypedProperties props
  ) {
    if (metadataMap.containsKey(INTERNAL_META_ORDERING_FIELD)) {
      return (Comparable) metadataMap.get(INTERNAL_META_ORDERING_FIELD);
    }

    if (!recordOpt.isPresent()) {
      return 0;
    }

    String orderingFieldName = ConfigUtils.getOrderingField(props);
    Object value = getFieldValueFromIndexedRecord(recordOpt.get(), schema, orderingFieldName);
    return value != null ? (Comparable) value : 0;
  }

  @Override
  public HoodieRecord<IndexedRecord> constructHoodieRecord(
      Option<IndexedRecord> recordOpt,
      Map<String, Object> metadataMap
  ) {
    if (!recordOpt.isPresent()) {
      HoodieKey key = new HoodieKey((String) metadataMap.get(INTERNAL_META_RECORD_KEY),
          (String) metadataMap.get(INTERNAL_META_PARTITION_PATH));
      return new HoodieEmptyRecord<>(
          key,
          HoodieOperation.DELETE,
          (Comparable<?>) metadataMap.get(INTERNAL_META_ORDERING_FIELD),
          HoodieRecord.HoodieRecordType.AVRO);
    }
    return new HoodieAvroIndexedRecord(recordOpt.get());
  }

  @Override
  public IndexedRecord seal(IndexedRecord record) {
    Schema schema = record.getSchema();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field, record.get(field.pos()));
    }
    return builder.build();
  }

  @Override
  public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, ClosableIterator<IndexedRecord> dataFileIterator) {
    return null;
  }

  @Override
  public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to) {
    return null;
  }

  private Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      Schema recordSchema,
      String fieldName
  ) {
    Schema.Field field = recordSchema.getField(fieldName);
    int pos = field.pos();
    return record.get(pos);
  }
}
