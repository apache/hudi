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

//package com.uber.hoodie.io.storage.log;
//
//import com.uber.hoodie.avro.HoodieAvroWriteSupport;
//import com.uber.hoodie.common.model.FileSlice;
//import com.uber.hoodie.common.model.HoodieLogFile;
//import com.uber.hoodie.common.model.HoodieRecord;
//import com.uber.hoodie.common.model.HoodieRecordLocation;
//import com.uber.hoodie.common.model.HoodieRecordPayload;
//import com.uber.hoodie.common.table.log.HoodieLogFormat;
//import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
//import com.uber.hoodie.common.table.log.HoodieLogFormatWriter;
//import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
//import com.uber.hoodie.common.util.FSUtils;
//import com.uber.hoodie.common.util.HoodieAvroUtils;
//import com.uber.hoodie.config.HoodieWriteConfig;
//import com.uber.hoodie.io.storage.HoodieParquetConfig;
//import com.uber.hoodie.io.storage.HoodieParquetWriter;
//import com.uber.hoodie.io.storage.HoodieStorageWriter;
//import com.uber.hoodie.io.storage.HoodieWrapperFileSystem;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Optional;
//import java.util.concurrent.atomic.AtomicLong;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.IndexedRecord;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.hadoop.ParquetFileWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.spark.TaskContext;
//
///**
// * HoodieParquetWriter extends the ParquetWriter to help limit the size of underlying file. Provides
// * a way to check if the current file can take more records with the <code>canWrite()</code>
// */
//public class HoodieLogWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
//    implements HoodieStorageWriter<R> {
//
//  private final Path file;
//  private final HoodieWrapperFileSystem fs;
//  private final long maxFileSize;
//  private final HoodieAvroWriteSupport writeSupport;
//  private final String commitTime;
//  private final Schema schema;
//  private HoodieLogFormatWriter writer;
//  private HoodieWriteConfig config;
//
//
//  public HoodieLogWriter(String commitTime, Path file,
//      Schema schema, Configuration configuration, HoodieWriteConfig config) throws IOException, InterruptedException {
//    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, configuration);
//    this.fs = (HoodieWrapperFileSystem) this.file
//        .getFileSystem(HoodieParquetWriter.registerFileSystem(file, configuration));
//    // We cannot accurately measure the snappy compressed output file size. We are choosing a
//    // conservative 10%
//    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
//    // stream and the actual file size reported by HDFS
//    this.maxFileSize = config.getParquetMaxFileSize() + Math
//        .round(config.getParquetBlockSize() * config.getParquetCompressionRatio());
//    this.writeSupport = null;
//    this.commitTime = commitTime;
//    this.schema = schema;
//    this.writer = createLogWriter(0, "");
//    this.config = config;
//  }
//
//  @Override
//  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
//    throw new UnsupportedOperationException("Log Block implementation");
//  }
//
//  public boolean canWrite() {
//    return fs.getBytesWritten(file) < maxFileSize;
//  }
//
//  @Override
//  public void writeAvro(String key, IndexedRecord object) throws IOException {
//  }
//
//  public void writeLogBlock(HoodieLogBlock hoodieLogBlock) throws IOException, InterruptedException {
//    this.writer.appendBlock(hoodieLogBlock);
//  }
//
//  private HoodieLogFormatWriter createLogWriter(int logVersion, String baseCommitTime)
//      throws IOException, InterruptedException {
//    return HoodieLogFormat.newWriterBuilder()
//        .onParentPath(new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath))
//        .withFileId(fileId).overBaseCommit(baseCommitTime).withLogVersion(logVersion)
//        .withSizeThreshold(config.getLogFileMaxSize()).withFs(fs)
//        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
//  }
//
//  @Override
//  public void close() {
//
//  }
//
//}
