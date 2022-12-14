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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * {@link HoodieFileReader} implementation for parquet format.
 *
 * @param <R> Record implementation that permits field access by integer index.
 */
public class HoodieAvroParquetReader extends HoodieAvroFileReaderBase {

  private final Path path;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;
  private final List<ParquetReaderIterator> readerIterators = new ArrayList<>();

  public HoodieAvroParquetReader(Configuration configuration, Path path) {
    // We have to clone the Hadoop Config as it might be subsequently modified
    // by the Reader (for proper config propagation to Parquet components)
    this.conf = tryOverrideDefaultConfigs(new Configuration(configuration));
    this.path = path;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  }

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(Schema readerSchema) throws IOException {
    // TODO(HUDI-4588) remove after HUDI-4588 is resolved
    // NOTE: This is a workaround to avoid leveraging projection w/in [[AvroParquetReader]],
    //       until schema handling issues (nullability canonicalization, etc) are resolved
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(readerSchema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema schema) throws IOException {
    return getIndexedRecordIteratorInternal(schema, Option.empty());
  }

  @Override
  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    return getIndexedRecordIteratorInternal(readerSchema, Option.of(requestedSchema));
  }

  @Override
  public Schema getSchema() {
    return parquetUtils.readAvroSchema(conf, path);
  }

  @Override
  public void close() {
    readerIterators.forEach(ParquetReaderIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(conf, path);
  }

  private static Configuration tryOverrideDefaultConfigs(Configuration conf) {
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
    if (conf.get(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS) == null) {
      conf.set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS,
          "false", "Overriding default treatment of repeated groups in Parquet");
    }

    if (conf.get(ParquetInputFormat.STRICT_TYPE_CHECKING) == null) {
      conf.set(ParquetInputFormat.STRICT_TYPE_CHECKING, "false",
          "Overriding default setting of whether type-checking is strict in Parquet reader, "
              + "to enable type promotions (in schema evolution)");
    }

    return conf;
  }

  private ClosableIterator<IndexedRecord> getIndexedRecordIteratorInternal(Schema schema, Option<Schema> requestedSchema) throws IOException {
    // NOTE: We have to set both Avro read-schema and projection schema to make
    //       sure that in case the file-schema is not equal to read-schema we'd still
    //       be able to read that file (in case projection is a proper one)
    if (!requestedSchema.isPresent()) {
      AvroReadSupport.setAvroReadSchema(conf, schema);
      AvroReadSupport.setRequestedProjection(conf, schema);
    } else {
      AvroReadSupport.setAvroReadSchema(conf, requestedSchema.get());
      AvroReadSupport.setRequestedProjection(conf, requestedSchema.get());
    }
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(path).withConf(conf).build();
    ParquetReaderIterator<IndexedRecord> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }
}
