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

import org.apache.hudi.hadoop.realtime.HoodieMergeOnReadTableInputFormat;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * !!! PLEASE READ CAREFULLY !!!
 *
 * NOTE: Hive bears optimizations which are based upon validating whether {@link FileInputFormat}
 * implementation inherits from {@link MapredParquetInputFormat}.
 *
 * To make sure that Hudi implementations are leveraging these optimizations to the fullest, this class
 * serves as a base-class for every {@link FileInputFormat} implementations working with Parquet file-format.
 *
 * However, this class serves as a simple delegate to the actual implementation hierarchy: it expects
 * either {@link HoodieCopyOnWriteTableInputFormat} or {@link HoodieMergeOnReadTableInputFormat} to be supplied
 * to which it delegates all of its necessary methods.
 */
public abstract class HoodieParquetInputFormatBase extends MapredParquetInputFormat implements Configurable {

  private final HoodieTableInputFormat inputFormatDelegate;

  protected HoodieParquetInputFormatBase(HoodieCopyOnWriteTableInputFormat inputFormatDelegate) {
    this.inputFormatDelegate = inputFormatDelegate;
  }

  @Override
  public final void setConf(Configuration conf) {
    inputFormatDelegate.setConf(conf);
  }

  @Override
  public final Configuration getConf() {
    return inputFormatDelegate.getConf();
  }

  @Override
  public final InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return inputFormatDelegate.getSplits(job, numSplits);
  }

  @Override
  protected final boolean isSplitable(FileSystem fs, Path filename) {
    return inputFormatDelegate.isSplitable(fs, filename);
  }

  @Override
  protected final FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
    return inputFormatDelegate.makeSplit(file, start, length, hosts);
  }

  @Override
  protected final FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts, String[] inMemoryHosts) {
    return inputFormatDelegate.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  @Override
  public final FileStatus[] listStatus(JobConf job) throws IOException {
    return inputFormatDelegate.listStatus(job);
  }
}
