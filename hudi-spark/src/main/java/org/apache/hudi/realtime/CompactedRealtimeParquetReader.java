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

package org.apache.hudi.realtime;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;

import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.RecordMaterializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;

/**
 * This is the actual record reader to read parquet file and scan the hudi log file.
 * @param <T>
 */
public class CompactedRealtimeParquetReader<T> extends AbstractRealtimeParquetReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactedRealtimeParquetReader.class);

  // The Map to store records from the log file. Used by the HoodieParquetRecordReaderIterator.
  public final Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecordMap;

  public CompactedRealtimeParquetReader(ReadSupport<T> readSupport, Filter filter, HoodieRealtimeFileSplit split, JobConf job)
      throws IOException {
    super(readSupport, filter, split, job);
    this.deltaRecordMap = getMergedLogRecordScanner().getRecords();
  }

  public CompactedRealtimeParquetReader(ReadSupport<T> readSupport, HoodieRealtimeFileSplit split, JobConf job)
      throws IOException {
    this(readSupport, FilterCompat.NOOP, split, job);
  }

  /**
   * Goes through the log files and populates a map with latest version of each key logged, since the base split was
   * written.
   */
  private HoodieMergedLogRecordScanner getMergedLogRecordScanner() throws IOException {
    // NOTE: HoodieCompactedLogRecordScanner will not return records for an in-flight commit
    // but can return records for completed commits > the commit we are trying to read (if using
    // readCommit() API)
    return new HoodieMergedLogRecordScanner(
        FSUtils.getFs(split.getPath().toString(), jobConf),
        split.getBasePath(),
        split.getDeltaLogPaths(),
        getLogAvroSchema(), // currently doesn't support custom payload, use schema from the log file as default
        split.getMaxCommitTime(),
        getMaxCompactionMemoryInBytes(),
        Boolean.parseBoolean(jobConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)),
        false,
        jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
        jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean recordFound = false;

    while (!recordFound) {
      // no more records left
      if (current >= total) {
        return false;
      }

      try {
        checkRead();
        current++;

        try {
          currentValue = recordReader.read();
        } catch (RecordMaterializer.RecordMaterializationException e) {
          // this might throw, but it's fatal if it does.
          unmaterializableRecordCounter.incErrors(e);
          LOG.debug("skipping a corrupt record");
          continue;
        }

        if (recordReader.shouldSkipCurrentRecord()) {
          // this record is being filtered via the filter2 package
          LOG.debug("skipping record");
          continue;
        }

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = totalCountLoadedSoFar;
          LOG.debug("filtered record reader reached end of block");
          continue;
        }

        recordFound = true;
        LOG.debug("read value: {}", currentValue);
      } catch (RuntimeException e) {
        throw new ParquetDecodingException(format("Can not read value at %d in block %d in file %s", current, currentBlock, reader.getPath()), e);
      }
    }
    return true;
  }
}
