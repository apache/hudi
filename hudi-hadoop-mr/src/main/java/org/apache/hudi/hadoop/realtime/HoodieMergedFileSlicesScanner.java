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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.arrayWritableToString;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.avroToArrayWritable;

/**
 * Scans through all files in a file slice. Constructs a map of records from Parquet file if present. Constructs merged
 * log records if log files are present in the slice. Merges records from log files on top of records from base parquet
 * file.
 *
 */
public class HoodieMergedFileSlicesScanner {

  private static final Logger LOG = LogManager.getLogger(HoodieMergedFileSlicesScanner.class);

  private final transient MapredParquetInputFormat mapredParquetInputFormat;
  private final HoodieMORIncrementalFileSplit split;
  private final List<FileSlice> fileSlices;

  // Final map of compacted/merged records
  // TODO change map to external spillable map. But ArrayWritable is not implementing Serializable
  private final Map<String, ArrayWritable> records;

  private final JobConf jobConf;
  private final Reporter reporter;
  private final HoodieMORIncrementalRecordReader recordReader;

  public HoodieMergedFileSlicesScanner(
          HoodieMORIncrementalFileSplit split,
          JobConf jobConf,
          Reporter reporter,
          HoodieMORIncrementalRecordReader recordReader) {
    this.split = split;
    this.fileSlices = split.getFileSlices()
        .stream()
        .sorted(Comparator.comparing(FileSlice::getBaseInstantTime))
        .collect(Collectors.toList());
    this.mapredParquetInputFormat = new MapredParquetInputFormat();
    this.records = new HashMap<>();
    this.jobConf = jobConf;
    this.reporter = reporter;
    this.recordReader = recordReader;
  }

  public void scan() throws IOException {
    for (FileSlice fileSlice: fileSlices) {
      Map<String, ArrayWritable> newRecords = scanFileSlice(fileSlice);
      mergeRecords(newRecords);
    }
  }

  private void mergeRecords(Map<String, ArrayWritable> newFileSliceRecords) {
    for (Entry<String, ArrayWritable> entry: newFileSliceRecords.entrySet()) {
      records.put(entry.getKey(), entry.getValue());
    }
  }

  private Map<String, ArrayWritable> scanFileSlice(FileSlice fileSlice) throws IOException {
    Map<String, ArrayWritable> baseFileRecords = fetchBaseFileRecordsMapForSlice(fileSlice);
    Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecords = fetchDeltaRecordMapForSlice(fileSlice);
    if ((baseFileRecords == null || baseFileRecords.isEmpty()) && deltaRecords != null) {
      Map<String, ArrayWritable> result = new HashMap<>();
      for (Entry<String, HoodieRecord<? extends HoodieRecordPayload>> entry: deltaRecords.entrySet()) {
        Option<ArrayWritable> aWritable = hoodieRecordToArrayWritable(entry.getValue());
        if (aWritable.isPresent()) {
          result.put(entry.getKey(), aWritable.get());
        }
      }
      return result;
    }
    if (deltaRecords == null && baseFileRecords != null) {
      return baseFileRecords;
    }
    // Apply delta records on top of base file records
    return applyUpdatesToBaseFile(baseFileRecords, deltaRecords);
  }

  private Map<String, ArrayWritable> applyUpdatesToBaseFile(
      Map<String, ArrayWritable> baseFileRecords,
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecords) throws IOException {
    Iterator<Entry<String, ArrayWritable>> iterator = baseFileRecords.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ArrayWritable> entry = iterator.next();
      if (deltaRecords.containsKey(entry.getKey())) {
        Option<ArrayWritable> aWritable = hoodieRecordToArrayWritable(deltaRecords.get(entry.getKey()));
        if (!aWritable.isPresent()) {
          // case where a valid record in basefile is deleted later in log files
          iterator.remove();
        } else {
          // replace original value with value from log file.
          Writable[] originalValue = entry.getValue().get();
          Writable[] replaceValue = aWritable.get().get();
          try {
            System.arraycopy(replaceValue, 0, originalValue, 0, originalValue.length);
            entry.getValue().set(originalValue);
          } catch (RuntimeException re) {
            LOG.error("Got exception when doing array copy", re);
            LOG.error("Base record :" + arrayWritableToString(entry.getValue()));
            LOG.error("Log record :" + arrayWritableToString(aWritable.get()));
            String errMsg = "Base-record :" + arrayWritableToString(entry.getValue())
                + " ,Log-record :" + arrayWritableToString(aWritable.get()) + " ,Error :" + re.getMessage();
            throw new RuntimeException(errMsg, re);
          }
        }
      }
    }
    return baseFileRecords;
  }

  private Option<ArrayWritable> hoodieRecordToArrayWritable(
      HoodieRecord<? extends HoodieRecordPayload> record) throws IOException {
    Option<GenericRecord> rec;
    if (recordReader.usesCustomPayload) {
      rec = record.getData().getInsertValue(recordReader.getWriterSchema());
    } else {
      rec = record.getData().getInsertValue(recordReader.getReaderSchema());
    }
    if (!rec.isPresent()) {
      // possibly the record is deleted
      return Option.empty();
    }
    GenericRecord recordToReturn = rec.get();
    if (recordReader.usesCustomPayload) {
      // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
      // the writerSchema with only the projection fields
      recordToReturn = HoodieAvroUtils.rewriteRecordWithOnlyNewSchemaFields(rec.get(), recordReader.getReaderSchema());
    }
    return Option.of((ArrayWritable) avroToArrayWritable(recordToReturn, recordReader.getHiveSchema()));
  }

  private Map<String, ArrayWritable> fetchBaseFileRecordsMapForSlice(FileSlice fileSlice) throws IOException {
    if (!fileSlice.getBaseFile().isPresent()) {
      return null;
    }

    // create a single split from the base parquet file and read the records.
    FileStatus baseFileStatus = fileSlice.getBaseFile().get().getFileStatus();
    FileSplit fileSplit = new FileSplit(baseFileStatus.getPath(), 0, baseFileStatus.getLen(), (String[]) null);
    Map<String, ArrayWritable> result = new HashMap<>();
    RecordReader<NullWritable, ArrayWritable> recordReader  = null;
    try {
      recordReader = mapredParquetInputFormat.getRecordReader(fileSplit, jobConf, reporter);
      NullWritable key = recordReader.createKey();
      ArrayWritable value = recordReader.createValue();
      boolean hasNext = recordReader.next(key, value);
      while (hasNext) {
        String hoodieRecordKey = value.get()[HOODIE_RECORD_KEY_COL_POS].toString();
        result.put(hoodieRecordKey, value);
        key = recordReader.createKey();
        value = recordReader.createValue();
        hasNext = recordReader.next(key, value);
      }
    } catch (IOException e) {
      LOG.error("Got exception when iterating parquet file: " + baseFileStatus.getPath(), e);
      throw new HoodieIOException("IO exception when reading parquet file");
    } finally {
      if (recordReader != null) {
        recordReader.close();
      }
    }
    return result;
  }

  private Map<String, HoodieRecord<? extends HoodieRecordPayload>> fetchDeltaRecordMapForSlice(FileSlice fileSlice) {
    if (!fileSlice.getLogFiles().findAny().isPresent()) {
      return null;
    }
    // reverse the log files to be in natural order
    List<String> logFilesPath = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).map(file -> file.getPath().toString()).collect(Collectors.toList());

    return new HoodieMergedLogRecordScanner(
        FSUtils.getFs(split.getBasePath(), jobConf),
        split.getBasePath(),
        logFilesPath,
        recordReader.usesCustomPayload ? recordReader.getWriterSchema() : recordReader.getReaderSchema(),
        split.getMaxCommitTime(),
        recordReader.getMaxCompactionMemoryInBytes(),
        Boolean
            .parseBoolean(jobConf.get(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)),
        false,
        jobConf.getInt(MAX_DFS_STREAM_BUFFER_SIZE_PROP, DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
        jobConf.get(SPILLABLE_MAP_BASE_PATH_PROP, DEFAULT_SPILLABLE_MAP_BASE_PATH)
    ).getRecords();
  }

  public Map<String, ArrayWritable> getRecords() {
    return records;
  }
}
