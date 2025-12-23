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

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public class HoodieAvroOrcWriter implements HoodieAvroFileWriter, Closeable {
  private static final AtomicLong RECORD_INDEX = new AtomicLong(1);

  private final long maxFileSize;
  private final HoodieSchema schema;
  private final List<TypeDescription> fieldTypes;
  private final List<String> fieldNames;
  private final VectorizedRowBatch batch;
  private final Writer writer;

  private final Path file;
  private final boolean isWrapperFileSystem;
  private final Option<HoodieWrapperFileSystem> wrapperFs;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;

  private final HoodieOrcConfig orcConfig;
  private String minRecordKey;
  private String maxRecordKey;

  public HoodieAvroOrcWriter(String instantTime, StoragePath file, HoodieOrcConfig config, HoodieSchema schema,
                             TaskContextSupplier taskContextSupplier) throws IOException {

    Configuration conf = HadoopFSUtils.registerFileSystem(file, config.getStorageConf().unwrapAs(Configuration.class));
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, conf);
    FileSystem fs = this.file.getFileSystem(conf);
    this.isWrapperFileSystem = fs instanceof HoodieWrapperFileSystem;
    this.wrapperFs = this.isWrapperFileSystem ? Option.of((HoodieWrapperFileSystem) fs) : Option.empty();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;

    this.schema = schema;
    final TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(this.schema);
    this.fieldTypes = orcSchema.getChildren();
    this.fieldNames = orcSchema.getFieldNames();
    this.maxFileSize = config.getMaxFileSize();
    this.batch = orcSchema.createRowBatch();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
        .blockSize(config.getBlockSize())
        .stripeSize(config.getStripeSize())
        .compress(config.getCompressionKind())
        .bufferSize(config.getBlockSize())
        .fileSystem(fs)
        .setSchema(orcSchema);
    this.writer = OrcFile.createWriter(this.file, writerOptions);
    this.orcConfig = config;
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    prepRecordWithMetadata(key, avroRecord, instantTime,
        taskContextSupplier.getPartitionIdSupplier().get(), RECORD_INDEX.getAndIncrement(), file.getName());
    writeAvro(key.getRecordKey(), avroRecord);
  }

  @Override
  public boolean canWrite() {
    return !isWrapperFileSystem || wrapperFs.get().getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String recordKey, IndexedRecord object) throws IOException {
    for (int col = 0; col < batch.numCols; col++) {
      ColumnVector colVector = batch.cols[col];
      final String thisField = fieldNames.get(col);
      final TypeDescription type = fieldTypes.get(col);

      Object fieldValue = ((GenericRecord) object).get(thisField);
      HoodieSchemaField field = schema.getField(thisField).get();
      AvroOrcUtils.addToVector(type, colVector, field.schema(), fieldValue, batch.size);
    }

    batch.size++;

    // Batch size corresponds to the number of written rows out of 1024 total rows (by default)
    // in the row batch, add the batch to file once all rows are filled and reset.
    if (batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
      batch.size = 0;
    }

    if (orcConfig.useBloomFilter()) {
      orcConfig.getBloomFilter().add(recordKey);
      if (minRecordKey != null) {
        minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
      } else {
        minRecordKey = recordKey;
      }

      if (maxRecordKey != null) {
        maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
      } else {
        maxRecordKey = recordKey;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    if (orcConfig.useBloomFilter()) {
      final BloomFilter bloomFilter = orcConfig.getBloomFilter();
      writer.addUserMetadata(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString())));
      if (minRecordKey != null && maxRecordKey != null) {
        writer.addUserMetadata(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, ByteBuffer.wrap(getUTF8Bytes(minRecordKey)));
        writer.addUserMetadata(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER, ByteBuffer.wrap(getUTF8Bytes(maxRecordKey)));
      }
      if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        writer.addUserMetadata(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE, ByteBuffer.wrap(getUTF8Bytes(bloomFilter.getBloomFilterTypeCode().name())));
      }
    }
    writer.addUserMetadata(HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY, ByteBuffer.wrap(getUTF8Bytes(schema.toString())));

    writer.close();
  }
}
