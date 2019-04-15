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
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.spark.TaskContext;

/**
 * HoodieParquetWriter provides a Parquet implementation of HoodieStorageWriter. Provides a way to check if the current
 * path can take more records with the <code>canWrite()</code>
 */
public class HoodieParquetWriter<T extends HoodieRecordPayload, R extends IndexedRecord> implements
    HoodieStorageWriter<R> {

  private static AtomicLong recordIndex = new AtomicLong(1);

  private final Configuration conf;
  private final Path path;
  private final ParquetWriter parquetWriter;
  private final HoodieWrapperFileSystem fs;
  private final HoodieAvroWriteSupport writeSupport;
  private final long maxFileSize;
  private final String commitTime;
  private final Schema schema;

  /**
   * Construct a HoodieParquetWriter instance.
   *
   * @param commitTime the commit time for these records
   * @param inputPath the path of the file to write
   * @param parquetConfig configs for the parquet writer
   * @param schema (UNUSED) the schema of the records
   */
  public HoodieParquetWriter(String commitTime, Path inputPath, HoodieParquetConfig parquetConfig, Schema schema)
      throws IOException {
    this.conf = registerFileSystem(inputPath, parquetConfig.getHadoopConf());
    this.path = HoodieWrapperFileSystem.convertToHoodiePath(inputPath, conf);
    HoodieOutputFile outputFile = HoodieOutputFile.fromPath(path, conf);
    this.parquetWriter = new ParquetWriterBuilder(outputFile).withParquetConfig(parquetConfig).withConf(conf)
        .withWriteMode(ParquetFileWriter.Mode.CREATE).build();
    this.fs = outputFile.getFileSystem();
    this.writeSupport = parquetConfig.getWriteSupport();
    // TODO: compute this compression ratio dynamically by looking at the bytes written to the stream
    //       and the actual path size reported by HDFS
    // We cannot accurately measure the snappy compressed output path size. We are choosing a conservative 10%
    this.maxFileSize = parquetConfig.getMaxFileSize() + Math
        .round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
    this.commitTime = commitTime;
    this.schema = schema;
  }

  private static Configuration registerFileSystem(Path path, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(path.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(path) < maxFileSize;
  }

  @Override
  public void close() throws IOException {
    parquetWriter.close();
  }

  @Override
  public void writeAvro(String key, R object) throws IOException {
    parquetWriter.write(object);
    writeSupport.add(key);
  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    String seqId = HoodieRecord
        .generateSequenceId(commitTime, TaskContext.getPartitionId(), recordIndex.getAndIncrement());
    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, record.getRecordKey(), record.getPartitionPath(),
        path.getName());
    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, commitTime, seqId);
    parquetWriter.write(avroRecord);
    writeSupport.add(record.getRecordKey());
  }

  private static class ParquetWriterBuilder extends ParquetWriter.Builder<IndexedRecord, ParquetWriterBuilder> {

    private HoodieParquetConfig parquetConfig;

    private ParquetWriterBuilder(OutputFile file) {
      super(file);
    }

    private ParquetWriterBuilder withParquetConfig(HoodieParquetConfig hoodieParquetConfig) {
      this.parquetConfig = hoodieParquetConfig;
      super.withCompressionCodec(parquetConfig.getCompressionCodecName());
      super.withRowGroupSize(parquetConfig.getBlockSize());
      super.withPageSize(parquetConfig.getPageSize());
      super.withDictionaryPageSize(parquetConfig.getPageSize());
      return this;
    }

    @Override
    protected ParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<IndexedRecord> getWriteSupport(Configuration conf) {
      return parquetConfig.getWriteSupport();
    }

  }

}

