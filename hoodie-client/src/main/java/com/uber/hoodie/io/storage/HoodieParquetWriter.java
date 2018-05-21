/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.TaskContext;

/**
 * HoodieParquetWriter extends the ParquetWriter to help limit the size of underlying file. Provides
 * a way to check if the current file can take more records with the <code>canWrite()</code>
 */
public class HoodieParquetWriter<T extends HoodieRecordPayload, R extends IndexedRecord> extends
    ParquetWriter<IndexedRecord> implements HoodieStorageWriter<R> {

  private static AtomicLong recordIndex = new AtomicLong(1);

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final long maxFileSize;
  private final HoodieAvroWriteSupport writeSupport;
  private final String commitTime;
  private final Schema schema;


  public HoodieParquetWriter(String commitTime, Path file, HoodieParquetConfig parquetConfig,
      Schema schema) throws IOException {
    super(HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf()),
        ParquetFileWriter.Mode.CREATE, parquetConfig.getWriteSupport(),
        parquetConfig.getCompressionCodecName(), parquetConfig.getBlockSize(),
        parquetConfig.getPageSize(), parquetConfig.getPageSize(),
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetWriter.DEFAULT_WRITER_VERSION,
        registerFileSystem(file, parquetConfig.getHadoopConf()));
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf());
    this.fs = (HoodieWrapperFileSystem) this.file
        .getFileSystem(registerFileSystem(file, parquetConfig.getHadoopConf()));
    // We cannot accurately measure the snappy compressed output file size. We are choosing a
    // conservative 10%
    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    this.maxFileSize = parquetConfig.getMaxFileSize() + Math
        .round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
    this.writeSupport = parquetConfig.getWriteSupport();
    this.commitTime = commitTime;
    this.schema = schema;
  }

  public static Configuration registerFileSystem(Path file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
        recordIndex.getAndIncrement());
    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, record.getRecordKey(),
        record.getPartitionPath(), file.getName());
    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, commitTime, seqId);
    super.write(avroRecord);
    writeSupport.add(record.getRecordKey());
  }

  public boolean canWrite() {
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String key, IndexedRecord object) throws IOException {
    super.write(object);
    writeSupport.add(key);
  }
}
