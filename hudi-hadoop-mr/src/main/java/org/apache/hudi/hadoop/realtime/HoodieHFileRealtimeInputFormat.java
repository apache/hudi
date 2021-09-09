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

import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.HoodieHFileInputFormat;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hudi.hadoop.UseRecordReaderFromInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * HoodieRealtimeInputFormat for HUDI datasets which store data in HFile base file format.
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieHFileRealtimeInputFormat extends HoodieHFileInputFormat {

  private static final Logger LOG = LogManager.getLogger(HoodieHFileRealtimeInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Stream<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits)).map(is -> (FileSplit) is);
    return HoodieRealtimeInputFormatUtils.getRealtimeSplits(job, fileSplits);
  }

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Call the HoodieInputFormat::listStatus to obtain all latest hfiles, based on commit timeline.
    return super.listStatus(job);
  }

  @Override
  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    // no specific filtering for Realtime format
    return timeline;
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf jobConf,
      final Reporter reporter) throws IOException {
    // Hive on Spark invokes multiple getRecordReaders from different threads in the same spark task (and hence the
    // same JVM) unlike Hive on MR. Due to this, accesses to JobConf, which is shared across all threads, is at the
    // risk of experiencing race conditions. Hence, we synchronize on the JobConf object here. There is negligible
    // latency incurred here due to the synchronization since get record reader is called once per spilt before the
    // actual heavy lifting of reading the parquet files happen.
    if (jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null) {
      synchronized (jobConf) {
        LOG.info(
            "Before adding Hoodie columns, Projections :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
                + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        if (jobConf.get(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP) == null) {
          // Hive (across all versions) fails for queries like select count(`_hoodie_commit_time`) from table;
          // In this case, the projection fields gets removed. Looking at HiveInputFormat implementation, in some cases
          // hoodie additional projection columns are reset after calling setConf and only natural projections
          // (one found in select queries) are set. things would break because of this.
          // For e:g _hoodie_record_key would be missing and merge step would throw exceptions.
          // TO fix this, hoodie columns are appended late at the time record-reader gets built instead of construction
          // time.
          HoodieRealtimeInputFormatUtils.addRequiredProjectionFields(jobConf, Option.empty());

          this.conf = jobConf;
          this.conf.set(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP, "true");
        }
      }
    }
    HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(jobConf);

    LOG.info("Creating record reader with readCols :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
        + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    // sanity check
    ValidationUtils.checkArgument(split instanceof HoodieRealtimeFileSplit,
        "HoodieRealtimeRecordReader can only work on HoodieRealtimeFileSplit and not with " + split);

    return new HoodieRealtimeRecordReader((HoodieRealtimeFileSplit) split, jobConf,
        super.getRecordReader(split, jobConf, reporter));
  }
}