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

package org.apache.hudi.bench.writer;

import org.apache.hudi.common.io.storage.HoodieWrapperFileSystem;
import org.apache.hudi.io.storage.HoodieParquetWriter;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Implementation of {@link FileDeltaInputWriter} that writes avro records to the result file.
 */
public class AvroDeltaInputWriter implements FileDeltaInputWriter<GenericRecord> {

  public static final String AVRO_EXTENSION = ".avro";
  private static Logger log = Logger.getLogger(AvroDeltaInputWriter.class);
  // The maximum file size for an avro file before being rolled over to a new one
  private final Long maxFileSize;
  private final Configuration configuration;
  private HoodieWrapperFileSystem fs;
  // Path of the actual avro file
  private Path file;
  // Base input path to write avro files under
  // TODO : Make this bucketed so don't have a large number of files in a single directory
  private String basePath;
  private DatumWriter<IndexedRecord> writer;
  private DataFileWriter<IndexedRecord> dataFileWriter;
  private OutputStream output;
  private Schema schema;
  private WriteStats writeStats;
  private long recordsWritten = 0;

  // TODO : Handle failure case which may leave behind tons of small corrupt files
  public AvroDeltaInputWriter(Configuration configuration, String basePath, String schemaStr, Long maxFileSize)
      throws IOException {
    this.schema = Schema.parse(schemaStr);
    this.maxFileSize = maxFileSize;
    this.configuration = configuration;
    this.basePath = basePath;
  }

  @Override
  public void writeData(GenericRecord iData) throws IOException {
    this.dataFileWriter.append(iData);
    recordsWritten++;
  }

  @Override
  public void open() throws IOException {
    Path path = new Path(basePath, new Path(UUID.randomUUID().toString() + AVRO_EXTENSION));
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(path, configuration);
    this.fs = (HoodieWrapperFileSystem) this.file
        .getFileSystem(HoodieParquetWriter.registerFileSystem(path, configuration));
    this.output = this.fs.create(this.file);
    this.writer = new GenericDatumWriter(schema);
    this.dataFileWriter = new DataFileWriter<>(writer).create(schema, output);
    this.writeStats = new WriteStats();
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void close() throws IOException {
    this.writeStats.setBytesWritten(this.fs.getBytesWritten(this.file));
    this.writeStats.setRecordsWritten(this.recordsWritten);
    this.writeStats.setFilePath(this.file.toUri().getPath());
    this.dataFileWriter.close();
    log.info("New Avro File => " + getPath());
  }

  @Override
  public FileDeltaInputWriter getNewWriter() throws IOException {
    AvroDeltaInputWriter avroDeltaInputWriter = new AvroDeltaInputWriter(this.configuration, this.basePath, this
        .schema.toString(), this.maxFileSize);
    avroDeltaInputWriter.open();
    return avroDeltaInputWriter;
  }

  public FileSystem getFs() {
    return fs;
  }

  public Path getPath() {
    return this.file;
  }

  @Override
  public WriteStats getWriteStats() {
    return this.writeStats;
  }
}
