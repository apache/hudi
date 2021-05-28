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

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Realtime Record Reader which can do compacted (merge-on-read) record reading or unmerged reading (parquet and log
 * files read in parallel) based on job configuration.
 */
public class HoodieRealtimeRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  // Property to enable parallel reading of parquet and log files without merging.
  public static final String REALTIME_SKIP_MERGE_PROP = "hoodie.realtime.merge.skip";
  // By default, we do merged-reading
  public static final String DEFAULT_REALTIME_SKIP_MERGE = "false";
  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeRecordReader.class);
  private final RecordReader<NullWritable, ArrayWritable> reader;

  public HoodieRealtimeRecordReader(RealtimeSplit split, JobConf job,
      RecordReader<NullWritable, ArrayWritable> realReader) {
    this.reader = constructRecordReader(split, job, realReader);
  }

  public static boolean canSkipMerging(JobConf jobConf) {
    return Boolean.parseBoolean(jobConf.get(REALTIME_SKIP_MERGE_PROP, DEFAULT_REALTIME_SKIP_MERGE));
  }

  /**
   * Construct record reader based on job configuration.
   *
   * @param split File Split
   * @param jobConf Job Configuration
   * @param realReader Parquet Record Reader
   * @return Realtime Reader
   */
  private static RecordReader<NullWritable, ArrayWritable> constructRecordReader(RealtimeSplit split,
      JobConf jobConf, RecordReader<NullWritable, ArrayWritable> realReader) {
    try {
      if (canSkipMerging(jobConf)) {
        LOG.info("Enabling un-merged reading of realtime records");
        return new RealtimeUnmergedRecordReader(split, jobConf, realReader);
      }
      LOG.info("Enabling merged reading of realtime records for split " + split);
      return new RealtimeCompactedRecordReader(split, jobConf, realReader);
    } catch (IOException ex) {
      LOG.error("Got exception when constructing record reader", ex);
      throw new HoodieException(ex);
    }
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    return this.reader.next(key, value);
  }

  @Override
  public NullWritable createKey() {
    return this.reader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return this.reader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return this.reader.getPos();
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return this.reader.getProgress();
  }

  public RecordReader<NullWritable, ArrayWritable> getReader() {
    return this.reader;
  }
}
