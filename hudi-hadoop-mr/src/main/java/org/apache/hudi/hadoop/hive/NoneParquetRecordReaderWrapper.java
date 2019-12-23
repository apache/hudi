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

package org.apache.hudi.hadoop.hive;

import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * If there is no data, it will support a none-parquet record reader for hive.
 */
public class NoneParquetRecordReaderWrapper extends ParquetRecordReaderBase
        implements RecordReader<NullWritable, ArrayWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(NoneParquetRecordReaderWrapper.class);

  public NoneParquetRecordReaderWrapper() {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return null;
  }

  @Override
  public long getPos() throws IOException {
    return 1L;
  }

  @Override
  public float getProgress() throws IOException {
    return 1f;
  }

  @Override
  public boolean next(final NullWritable key, final ArrayWritable value) throws IOException {
    return false;
  }
}
