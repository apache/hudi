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

package org.apache.hudi.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Record Reader for parquet. Records read from this reader is safe to be buffered for concurrent processing.
 *
 * In concurrent producer/consumer pattern, where the record is read and buffered by one thread and processed in another
 * thread, we need to ensure new instance of ArrayWritable is buffered. ParquetReader createKey/Value is unsafe as it
 * gets reused for subsequent fetch. This wrapper makes ParquetReader safe for this use-case.
 */
public class SafeParquetRecordReaderWrapper implements RecordReader<NullWritable, ArrayWritable> {

  // real Parquet reader to be wrapped
  private final RecordReader<NullWritable, ArrayWritable> parquetReader;

  // Value Class
  private final Class valueClass;

  // Number of fields in Value Schema
  private final int numValueFields;

  public SafeParquetRecordReaderWrapper(RecordReader<NullWritable, ArrayWritable> parquetReader) {
    this.parquetReader = parquetReader;
    ArrayWritable arrayWritable = parquetReader.createValue();
    this.valueClass = arrayWritable.getValueClass();
    this.numValueFields = arrayWritable.get().length;
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    return parquetReader.next(key, value);
  }

  @Override
  public NullWritable createKey() {
    return parquetReader.createKey();
  }

  /**
   * We could be in concurrent fetch and read env. We need to ensure new ArrayWritable as ParquetReader implementation
   * reuses same ArrayWritable for all reads which will cause corruption when buffering. So, we create a new
   * ArrayWritable here with Value class from parquetReader's value and an empty array.
   */
  @Override
  public ArrayWritable createValue() {
    // Call createValue of parquetReader to get size and class type info only
    Writable[] emptyWritableBuf = new Writable[numValueFields];
    return new ArrayWritable(valueClass, emptyWritableBuf);
  }

  @Override
  public long getPos() throws IOException {
    return parquetReader.getPos();
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return parquetReader.getProgress();
  }
}
