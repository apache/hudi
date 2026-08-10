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

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * HoodieInputFormat for HUDI datasets which store data in Lance base file format.
 * <p>
 * This class is required for catalog/metastore registration during CREATE TABLE operations.
 * <p>
 * TODO(#18557): Lance reading through Hive InputFormat is not yet supported. When support is
 * added, this should route through {@link HoodieFileGroupReaderBasedRecordReader} (like
 * {@link HoodieParquetInputFormat} does) instead of a standalone record reader,
 * to get MOR log merging, schema evolution, and bootstrap support for free.
 *
 * @see <a href="https://github.com/apache/hudi/issues/18557">#18557</a>
 */
@UseFileSplitsFromInputFormat
public class HoodieLanceInputFormat extends HoodieCopyOnWriteTableInputFormat {

  protected HoodieTimeline filterInstantsTimeline(HoodieTimeline timeline) {
    return HoodieInputFormatUtils.filterInstantsTimeline(timeline);
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf job,
      final Reporter reporter) throws IOException {
    throw new UnsupportedOperationException(
        "Lance reading through Hive InputFormat is not yet supported. "
            + "Use the Spark datasource path (spark.read.format(\"hudi\")) to read Lance tables.");
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // Lance files are not splittable.
    return false;
  }
}
