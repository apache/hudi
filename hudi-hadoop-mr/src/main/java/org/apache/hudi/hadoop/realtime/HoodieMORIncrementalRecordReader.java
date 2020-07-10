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

package org.apache.hudi.hadoop.realtime;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Record Reader which can do compacted (merge-on-read) record reading to serve incremental queries.
 */
public class HoodieMORIncrementalRecordReader extends AbstractRealtimeRecordReader implements RecordReader<NullWritable, ArrayWritable> {
  private static final Logger LOG = LogManager.getLogger(HoodieMORIncrementalRecordReader.class);
  private final Map<String, ArrayWritable> records;
  private final Iterator<ArrayWritable> iterator;

  public HoodieMORIncrementalRecordReader(HoodieMORIncrementalFileSplit split, JobConf job, Reporter reporter) {
    super(split, job);
    HoodieMergedFileSlicesScanner fileSlicesScanner = new HoodieMergedFileSlicesScanner(split, job, reporter, this);
    try {
      fileSlicesScanner.scan();
      this.records = fileSlicesScanner.getRecords();
      this.iterator = records.values().iterator();
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading log file ");
    }
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    if (!iterator.hasNext()) {
      return false;
    }
    value.set(iterator.next().get());
    return true;
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(Writable.class);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
