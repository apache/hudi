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
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.hive.util.SchemaUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;

/**
 * CLI command to display log file options.
 */
@Component
public class HoodieLogFileCommand implements CommandMarker {

  @CliAvailabilityIndicator({"show logfiles"})
  public boolean isShowArchivedLogFileAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

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

    FileSystem fs = HoodieCLI.tableMetadata.getFs();
    List<String> logFilePaths = Arrays.stream(fs.globStatus(new Path(logFilePathPattern)))
        .map(status -> status.getPath().toString()).collect(Collectors.toList());
    Map<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> commitCountAndMetadata =
        Maps.newHashMap();
    int totalEntries = 0;
    int numCorruptBlocks = 0;
    int dummyInstantTimeCount = 0;

    for (String logFilePath : logFilePaths) {
      FileStatus[] fsStatus = fs.listStatus(new Path(logFilePath));
      Schema writerSchema = new AvroSchemaConverter()
          .convert(SchemaUtil.readSchemaFromLogFile(HoodieCLI.tableMetadata.getFs(), new Path(logFilePath)));
      Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(fsStatus[0].getPath()), writerSchema);

      // read the avro blocks
      while (reader.hasNext()) {
        HoodieLogBlock n = reader.next();
        String instantTime;
        int recordCount = 0;
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
          if (n instanceof HoodieAvroDataBlock) {
            recordCount = ((HoodieAvroDataBlock) n).getRecords().size();
          }
        }
        if (commitCountAndMetadata.containsKey(instantTime)) {
          commitCountAndMetadata.get(instantTime).add(
              new Tuple3<>(n.getBlockType(), new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount));
          totalEntries++;
        } else {
          List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>> list =
              new ArrayList<>();
          list.add(
              new Tuple3<>(n.getBlockType(), new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount));
          commitCountAndMetadata.put(instantTime, list);
          totalEntries++;
        }
      }
      reader.close();
    }
    List<Comparable[]> rows = new ArrayList<>();
    int i = 0;
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map.Entry<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> entry : commitCountAndMetadata
        .entrySet()) {
      String instantTime = entry.getKey().toString();
      for (Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer> tuple3 : entry
          .getValue()) {
        Comparable[] output = new Comparable[5];
        output[0] = instantTime;
        output[1] = tuple3._3();
        output[2] = tuple3._1().toString();
        output[3] = objectMapper.writeValueAsString(tuple3._2()._1());
        output[4] = objectMapper.writeValueAsString(tuple3._2()._2());
        rows.add(output);
        i++;
      }
    }

    TableHeader header = new TableHeader().addTableHeaderField("InstantTime").addTableHeaderField("RecordCount")
        .addTableHeaderField("BlockType").addTableHeaderField("HeaderMetadata").addTableHeaderField("FooterMetadata");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "show logfile records", help = "Read records from log files")
  public String showLogFileRecords(
      @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits",
          unspecifiedDefaultValue = "10") final Integer limit,
      @CliOption(key = "logFilePathPattern", mandatory = true,
          help = "Fully qualified paths for the log files") final String logFilePathPattern,
      @CliOption(key = "mergeRecords", mandatory = false, help = "If the records in the log files should be merged",
          unspecifiedDefaultValue = "false") final Boolean shouldMerge)
      throws IOException {

    System.out.println("===============> Showing only " + limit + " records <===============");

    FileSystem fs = HoodieCLI.tableMetadata.getFs();
    List<String> logFilePaths = Arrays.stream(fs.globStatus(new Path(logFilePathPattern)))
        .map(status -> status.getPath().toString()).collect(Collectors.toList());

    // TODO : readerSchema can change across blocks/log files, fix this inside Scanner
    AvroSchemaConverter converter = new AvroSchemaConverter();
    // get schema from last log file
    Schema readerSchema =
        converter.convert(SchemaUtil.readSchemaFromLogFile(fs, new Path(logFilePaths.get(logFilePaths.size() - 1))));

    List<IndexedRecord> allRecords = new ArrayList<>();

    if (shouldMerge) {
      System.out.println("===========================> MERGING RECORDS <===================");
      HoodieMergedLogRecordScanner scanner =
          new HoodieMergedLogRecordScanner(fs, HoodieCLI.tableMetadata.getBasePath(), logFilePaths, readerSchema,
              HoodieCLI.tableMetadata.getActiveTimeline().getCommitTimeline().lastInstant().get().getTimestamp(),
              Long.valueOf(HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES),
              Boolean.valueOf(HoodieCompactionConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED),
              Boolean.valueOf(HoodieCompactionConfig.DEFAULT_COMPACTION_REVERSE_LOG_READ_ENABLED),
              Integer.valueOf(HoodieMemoryConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
              HoodieMemoryConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH);
      for (HoodieRecord<? extends HoodieRecordPayload> hoodieRecord : scanner) {
        Option<IndexedRecord> record = hoodieRecord.getData().getInsertValue(readerSchema);
        if (allRecords.size() >= limit) {
          break;
        }
        allRecords.add(record.get());
      }
    } else {
      for (String logFile : logFilePaths) {
        Schema writerSchema = new AvroSchemaConverter()
            .convert(SchemaUtil.readSchemaFromLogFile(HoodieCLI.tableMetadata.getFs(), new Path(logFile)));
        HoodieLogFormat.Reader reader =
            HoodieLogFormat.newReader(fs, new HoodieLogFile(new Path(logFile)), writerSchema);
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieLogBlock n = reader.next();
          if (n instanceof HoodieAvroDataBlock) {
            HoodieAvroDataBlock blk = (HoodieAvroDataBlock) n;
            List<IndexedRecord> records = blk.getRecords();
            allRecords.addAll(records);
            if (allRecords.size() >= limit) {
              break;
            }
          }
        }
        reader.close();
        if (allRecords.size() >= limit) {
          break;
        }
      }
    }
    String[][] rows = new String[allRecords.size() + 1][];
    int i = 0;
    for (IndexedRecord record : allRecords) {
      String[] data = new String[1];
      data[0] = record.toString();
      rows[i] = data;
      i++;
    }
    return HoodiePrintHelper.print(new String[] {"Records"}, rows);
  }
}
