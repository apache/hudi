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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMemoryConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * CLI command to display log file options.
 */
@Component
public class HoodieLogFileCommand implements CommandMarker {

  @CliCommand(value = "show logfile metadata", help = "Read commit metadata from log files")
  public String showLogFileCommits(
      @CliOption(key = "logFilePathPattern", mandatory = true,
          help = "Fully qualified path for the log file") final String logFilePathPattern,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only",
          unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    FileSystem fs = HoodieCLI.getTableMetaClient().getFs();
    List<String> logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(logFilePathPattern)).stream()
        .map(status -> status.getPath().toString()).collect(Collectors.toList());
    Map<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> commitCountAndMetadata =
        new HashMap<>();
    int numCorruptBlocks = 0;
    int dummyInstantTimeCount = 0;

    for (String logFilePath : logFilePaths) {
      FileStatus[] fsStatus = fs.listStatus(new Path(logFilePath));
      Schema writerSchema = new AvroSchemaConverter()
          .convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePath))));
      Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(fsStatus[0].getPath()), writerSchema);

      // read the avro blocks
      while (reader.hasNext()) {
        HoodieLogBlock n = reader.next();
        String instantTime;
        AtomicInteger recordCount = new AtomicInteger(0);
        if (n instanceof HoodieCorruptBlock) {
          try {
            instantTime = n.getLogBlockHeader().get(HeaderMetadataType.INSTANT_TIME);
            if (instantTime == null) {
              throw new Exception("Invalid instant time " + instantTime);
            }
          } catch (Exception e) {
            numCorruptBlocks++;
            instantTime = "corrupt_block_" + numCorruptBlocks;
            // could not read metadata for corrupt block
          }
        } else {
          instantTime = n.getLogBlockHeader().get(HeaderMetadataType.INSTANT_TIME);
          if (instantTime == null) {
            // This can happen when reading archived commit files since they were written without any instant time
            dummyInstantTimeCount++;
            instantTime = "dummy_instant_time_" + dummyInstantTimeCount;
          }
          if (n instanceof HoodieDataBlock) {
            try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = ((HoodieDataBlock) n).getRecordIterator(HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(r -> recordCount.incrementAndGet());
            }
          }
        }
        if (commitCountAndMetadata.containsKey(instantTime)) {
          commitCountAndMetadata.get(instantTime).add(
              new Tuple3<>(n.getBlockType(), new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount.get()));
        } else {
          List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>> list =
              new ArrayList<>();
          list.add(
              new Tuple3<>(n.getBlockType(), new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount.get()));
          commitCountAndMetadata.put(instantTime, list);
        }
      }
      reader.close();
    }
    List<Comparable[]> rows = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map.Entry<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> entry : commitCountAndMetadata
        .entrySet()) {
      String instantTime = entry.getKey();
      for (Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer> tuple3 : entry
          .getValue()) {
        Comparable[] output = new Comparable[5];
        output[0] = instantTime;
        output[1] = tuple3._3();
        output[2] = tuple3._1().toString();
        output[3] = objectMapper.writeValueAsString(tuple3._2()._1());
        output[4] = objectMapper.writeValueAsString(tuple3._2()._2());
        rows.add(output);
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_RECORD_COUNT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BLOCK_TYPE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HEADER_METADATA)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FOOTER_METADATA);

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "show logfile records", help = "Read records from log files")
  public String showLogFileRecords(
      @CliOption(key = {"limit"}, help = "Limit commits",
          unspecifiedDefaultValue = "10") final Integer limit,
      @CliOption(key = "logFilePathPattern", mandatory = true,
          help = "Fully qualified paths for the log files") final String logFilePathPattern,
      @CliOption(key = "mergeRecords", help = "If the records in the log files should be merged",
          unspecifiedDefaultValue = "false") final Boolean shouldMerge)
      throws IOException {

    System.out.println("===============> Showing only " + limit + " records <===============");

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    FileSystem fs = client.getFs();
    List<String> logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(logFilePathPattern)).stream()
        .map(status -> status.getPath().toString()).sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());

    // logFilePaths size must > 1
    checkArgument(logFilePaths.size() > 0, "There is no log file");

    // TODO : readerSchema can change across blocks/log files, fix this inside Scanner
    AvroSchemaConverter converter = new AvroSchemaConverter();
    // get schema from last log file
    Schema readerSchema =
        converter.convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePaths.get(logFilePaths.size() - 1)))));

    List<IndexedRecord> allRecords = new ArrayList<>();

    if (shouldMerge) {
      System.out.println("===========================> MERGING RECORDS <===================");
      HoodieMergedLogRecordScanner scanner =
          HoodieMergedLogRecordScanner.newBuilder()
              .withFileSystem(fs)
              .withBasePath(client.getBasePath())
              .withLogFilePaths(logFilePaths)
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(
                  client.getActiveTimeline()
                      .getCommitTimeline().lastInstant().get().getTimestamp())
              .withReadBlocksLazily(
                  Boolean.parseBoolean(
                      HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE.defaultValue()))
              .withReverseReader(
                  Boolean.parseBoolean(
                      HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue()))
              .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue())
              .withMaxMemorySizeInBytes(
                  HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
              .withSpillableMapBasePath(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.defaultValue())
              .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue())
              .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue())
              .withRecordMerger(HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName()))
              .build();
      for (HoodieRecord hoodieRecord : scanner) {
        Option<HoodieAvroIndexedRecord> record = hoodieRecord.toIndexedRecord(readerSchema, new Properties());
        if (allRecords.size() < limit) {
          allRecords.add(record.get().getData());
        }
      }
    } else {
      for (String logFile : logFilePaths) {
        Schema writerSchema = new AvroSchemaConverter()
            .convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(client.getFs(), new Path(logFile))));
        HoodieLogFormat.Reader reader =
            HoodieLogFormat.newReader(fs, new HoodieLogFile(new Path(logFile)), writerSchema);
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieLogBlock n = reader.next();
          if (n instanceof HoodieDataBlock) {
            HoodieDataBlock blk = (HoodieDataBlock) n;
            try (ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = blk.getRecordIterator(HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(record -> {
                if (allRecords.size() < limit) {
                  allRecords.add(record.getData());
                }
              });
            }
          }
        }
        reader.close();
        if (allRecords.size() >= limit) {
          break;
        }
      }
    }
    String[][] rows = new String[allRecords.size()][];
    int i = 0;
    for (IndexedRecord record : allRecords) {
      String[] data = new String[1];
      data[0] = record.toString();
      rows[i] = data;
      i++;
    }
    return HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_RECORDS}, rows);
  }
}
