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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ParquetReaderIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.util.Iterator;

public class HoodieParquetStreamReader<R extends IndexedRecord> {

  private final HoodieParquetInputFile parquetInputFile;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;

  public HoodieParquetStreamReader(Configuration configuration, FSDataInputStream inputStream) {
    this.conf = configuration;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
    parquetInputFile = new HoodieParquetInputFile(inputStream);
  }

  public Iterator<R> getRecordIterator(Schema schema) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, schema);
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(parquetInputFile)
        .withConf(conf).build();
    return new ParquetReaderIterator(reader);
  }

  private static class HoodieParquetInputFile implements InputFile {

    private final FSDataInputStream stream;

    public HoodieParquetInputFile(FSDataInputStream stream) {
      this.stream = stream;
    }

    @Override
    public long getLength() throws IOException {
      throw new UnsupportedOperationException("getLength is not implemented");
    }

    @Override
    public SeekableInputStream newStream() throws IOException  {
      return new ParquetInputStream(stream);
    }
  }

  private static class ParquetInputStream extends DelegatingSeekableInputStream {

    private final FSDataInputStream stream;

    public ParquetInputStream(FSDataInputStream stream) {
      super(stream);
      this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      stream.readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      stream.readFully(bytes);
    }
  }
}
