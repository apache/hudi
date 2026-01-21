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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.HoodieAvroParquetReaderIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.HoodieAvroParquetReaderBuilder;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.AvroSchemaRepair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

/**
 * {@link HoodieFileReader} implementation for parquet format.
 */
public class HoodieAvroParquetReader extends HoodieAvroFileReader {

  private final StoragePath path;
  private final HoodieStorage storage;
  private final FileFormatUtils parquetUtils;
  private final List<ParquetReaderIterator> readerIterators = new ArrayList<>();
  private Option<HoodieSchema> fileSchema = Option.empty();

  public HoodieAvroParquetReader(HoodieStorage storage, StoragePath path) {
    // We have to clone the Hadoop Config as it might be subsequently modified
    // by the Reader (for proper config propagation to Parquet components)
    this.storage = storage.newInstance(path, tryOverrideDefaultConfigs(storage.getConf().newInstance()));
    this.path = path;
    this.parquetUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET);
  }

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(HoodieSchema readerSchema) throws IOException {
    // TODO(HUDI-4588) remove after HUDI-4588 is resolved
    // NOTE: This is a workaround to avoid leveraging projection w/in [[AvroParquetReader]],
    //       until schema handling issues (nullability canonicalization, etc) are resolved
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(readerSchema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(storage, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(storage, path);
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(storage, path, candidateRowKeys);
  }

  @Override
  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(HoodieSchema schema) throws IOException {
    return getIndexedRecordIteratorInternal(schema, Collections.emptyMap());
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    return getIndexedRecordIteratorInternal(requestedSchema, Collections.emptyMap());
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema, Map<String, String> renamedColumns) throws IOException {
    return getIndexedRecordIteratorInternal(requestedSchema, renamedColumns);
  }

  @Override
  public HoodieSchema getSchema() {
    // Lazy initialization with caching: read schema from parquet file footer on first call,
    // then cache it in fileSchema to avoid repeated I/O on subsequent calls
    return fileSchema.orElseGet(() -> {
      HoodieSchema schema = parquetUtils.readSchema(storage, path);
      fileSchema = Option.ofNullable(schema);
      return schema;
    });
  }

  @Override
  public void close() {
    readerIterators.forEach(ParquetReaderIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(storage, path);
  }

  private static StorageConfiguration<?> tryOverrideDefaultConfigs(StorageConfiguration<?> conf) {
    // NOTE: Parquet uses elaborate encoding of the arrays/lists with optional types,
    //       following structure will be representing such list in Parquet:
    //
    //       optional group tip_history (LIST) {
    //         repeated group list {
    //           optional group element {
    //             optional double amount;
    //             optional binary currency (STRING);
    //           }
    //         }
    //       }
    //
    //       To designate list, special logical-type annotation (`LIST`) is used,
    //       as well additional [[GroupType]] with the name "list" is wrapping
    //       the "element" type (representing record stored inside the list itself).
    //
    //       By default [[AvroSchemaConverter]] would be interpreting any {@code REPEATED}
    //       Parquet [[GroupType]] as list, skipping the checks whether additional [[GroupType]]
    //       (named "list") is actually wrapping the "element" type therefore incorrectly
    //       converting it into an additional record-wrapper (instead of simply omitting it).
    //       To work this around we're
    //          - Checking whether [[AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS]] has been
    //          explicitly set in the Hadoop Config
    //          - In case it's not, we override the default value from "true" to "false"
    //
    if (conf.getString(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS).isEmpty()) {
      // Overriding default treatment of repeated groups in Parquet
      conf.set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, "false");
    }

    if (conf.getString(ParquetInputFormat.STRICT_TYPE_CHECKING).isEmpty()) {
      // Overriding default setting of whether type-checking is strict in Parquet reader,
      // to enable type promotions (in schema evolution)
      conf.set(ParquetInputFormat.STRICT_TYPE_CHECKING, "false");
    }

    return conf;
  }

  private ClosableIterator<IndexedRecord> getIndexedRecordIteratorInternal(HoodieSchema schema, Map<String, String> renamedColumns) throws IOException {
    // NOTE: We have to set both Avro read-schema and projection schema to make
    //       sure that in case the file-schema is not equal to read-schema we'd still
    //       be able to read that file (in case projection is a proper one)
    Configuration hadoopConf = storage.getConf().unwrapCopyAs(Configuration.class);
    //TODO boundary for now to revisit in later pr to use HoodieSchema
    Schema repairedFileSchema = AvroSchemaRepair.repairLogicalTypes(getSchema().toAvroSchema(), schema.toAvroSchema());
    Option<Schema> promotedSchema = Option.empty();
    if (!renamedColumns.isEmpty() || HoodieAvroUtils.recordNeedsRewriteForExtendedAvroTypePromotion(repairedFileSchema, schema.toAvroSchema())) {
      AvroReadSupport.setAvroReadSchema(hadoopConf, repairedFileSchema);
      AvroReadSupport.setRequestedProjection(hadoopConf, repairedFileSchema);
      promotedSchema = Option.of(schema.toAvroSchema());
    } else {
      AvroReadSupport.setAvroReadSchema(hadoopConf, schema.toAvroSchema());
      AvroReadSupport.setRequestedProjection(hadoopConf, schema.toAvroSchema());
    }
    ParquetReader<IndexedRecord> reader =
        new HoodieAvroParquetReaderBuilder<IndexedRecord>(path)
            .withTableSchema(getAvroSchemaConverter(hadoopConf).convert(schema))
            .withConf(hadoopConf)
            .set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, hadoopConf.get(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS))
            .set(ParquetInputFormat.STRICT_TYPE_CHECKING, hadoopConf.get(ParquetInputFormat.STRICT_TYPE_CHECKING))
            .build();
    ParquetReaderIterator<IndexedRecord> parquetReaderIterator = promotedSchema.isPresent()
        ? new HoodieAvroParquetReaderIterator(reader, HoodieSchema.fromAvroSchema(promotedSchema.get()), renamedColumns)
        : new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    ClosableIterator<IndexedRecord> recordKeyIterator = getIndexedRecordIterator(HoodieSchemaUtils.getRecordKeySchema());
    return new ClosableIterator<String>() {
      @Override
      public boolean hasNext() {
        return recordKeyIterator.hasNext();
      }

      @Override
      public String next() {
        Object obj = recordKeyIterator.next();
        return ((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      }

      @Override
      public void close() {
        recordKeyIterator.close();
      }
    };
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> sortedKeys,
                                                                         HoodieSchema readerSchema) {
    throw new UnsupportedOperationException("Not supported operation: getIndexedRecordsByKeysIterator");
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(List<String> sortedKeyPrefixes,
                                                                              HoodieSchema readerSchema) {
    throw new UnsupportedOperationException("Not supported operation: getIndexedRecordsByKeyPrefixIterator");
  }
}
