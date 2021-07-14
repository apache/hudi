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

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.AvroOrcUtils;

public class HoodieOrcWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
    implements HoodieFileWriter<R>, Closeable {
  private static final AtomicLong RECORD_INDEX = new AtomicLong(1);

  private final long maxFileSize;
  private final Schema avroSchema;
  private final List<TypeDescription> fieldTypes;
  private final List<String> fieldNames;
  private final VectorizedRowBatch batch;
  private final Writer writer;

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;

  private HoodieOrcConfig orcConfig;
  private String minRecordKey;
  private String maxRecordKey;

  public HoodieOrcWriter(String instantTime, Path file, HoodieOrcConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {

    Configuration conf = FSUtils.registerFileSystem(file, config.getHadoopConf());
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, conf);
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(conf);
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;

    this.avroSchema = schema;
    final TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(avroSchema);
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
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    prepRecordWithMetadata(avroRecord, record, instantTime,
        taskContextSupplier.getPartitionIdSupplier().get(), RECORD_INDEX, file.getName());
    writeAvro(record.getRecordKey(), avroRecord);
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String recordKey, IndexedRecord object) throws IOException {
    for (int col = 0; col < batch.numCols; col++) {
      ColumnVector colVector = batch.cols[col];
      final String thisField = fieldNames.get(col);
      final TypeDescription type = fieldTypes.get(col);

      Object fieldValue = ((GenericRecord) object).get(thisField);
      Schema.Field avroField = avroSchema.getField(thisField);
      AvroOrcUtils.addToVector(type, colVector, avroField.schema(), fieldValue, batch.size);
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
      writer.addUserMetadata(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, ByteBuffer.wrap(bloomFilter.serializeToString().getBytes()));
      if (minRecordKey != null && maxRecordKey != null) {
        writer.addUserMetadata(HOODIE_MIN_RECORD_KEY_FOOTER, ByteBuffer.wrap(minRecordKey.getBytes()));
        writer.addUserMetadata(HOODIE_MAX_RECORD_KEY_FOOTER, ByteBuffer.wrap(maxRecordKey.getBytes()));
      }
      if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
        writer.addUserMetadata(HOODIE_BLOOM_FILTER_TYPE_CODE, ByteBuffer.wrap(bloomFilter.getBloomFilterTypeCode().name().getBytes()));
      }
    }
    writer.addUserMetadata(HoodieOrcConfig.AVRO_SCHEMA_METADATA_KEY, ByteBuffer.wrap(avroSchema.toString().getBytes()));

    writer.close();
  }

  @Override
  public long getBytesWritten() {
    return fs.getBytesWritten(file);
  }
}
