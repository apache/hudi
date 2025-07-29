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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.FooterMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * CLI command to display log file options.
 */
@ShellComponent
public class HoodieLogFileCommand {

  @ShellMethod(key = "show logfile metadata", value = "Read commit metadata from log files")
  public String showLogFileCommits(
      @ShellOption(value = "--logFilePathPattern",
          help = "Fully qualified path for the log file") final String logFilePathPattern,
      @ShellOption(value = {"--limit"}, help = "Limit commits", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
              defaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieStorage storage = HoodieCLI.getTableMetaClient().getStorage();
    List<String> logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(
        storage, new StoragePath(logFilePathPattern)).stream()
        .map(status -> status.getPath().toString()).collect(Collectors.toList());
    Map<String, List<Tuple3<Tuple2<String, HoodieLogBlockType>, Tuple2<Map<HeaderMetadataType, String>,
        Map<FooterMetadataType, String>>, Integer>>> commitCountAndMetadata =
        new HashMap<>();
    int numCorruptBlocks = 0;
    int dummyInstantTimeCount = 0;
    String basePath = HoodieCLI.basePath;

    for (String logFilePath : logFilePaths) {
      StoragePath path = new StoragePath(logFilePath);
      String pathString = path.toString();
      String fileName;
      if (pathString.contains(basePath)) {
        String[] split = pathString.split(basePath);
        fileName = split[split.length - 1];
      } else {
        fileName = path.getName();
      }
      Schema writerSchema = TableSchemaResolver.readSchemaFromLogFile(storage, path);
      try (Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(path), writerSchema)) {

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
                new Tuple3<>(new Tuple2<>(fileName, n.getBlockType()),
                    new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount.get()));
          } else {
            List<Tuple3<Tuple2<String, HoodieLogBlockType>, Tuple2<Map<HeaderMetadataType, String>,
                Map<FooterMetadataType, String>>, Integer>> list =
                new ArrayList<>();
            list.add(
                new Tuple3<>(new Tuple2<>(fileName, n.getBlockType()),
                    new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount.get()));
            commitCountAndMetadata.put(instantTime, list);
          }
        }
      }
    }
    List<Comparable[]> rows = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map.Entry<String, List<Tuple3<Tuple2<String, HoodieLogBlockType>, Tuple2<Map<HeaderMetadataType, String>,
        Map<FooterMetadataType, String>>, Integer>>> entry : commitCountAndMetadata
        .entrySet()) {
      String instantTime = entry.getKey();
      for (Tuple3<Tuple2<String, HoodieLogBlockType>, Tuple2<Map<HeaderMetadataType, String>,
          Map<FooterMetadataType, String>>, Integer> tuple3 : entry
          .getValue()) {
        Comparable[] output = new Comparable[6];
        output[0] = tuple3._1()._1();
        output[1] = instantTime;
        output[2] = tuple3._3();
        output[3] = tuple3._1()._2().toString();
        output[4] = objectMapper.writeValueAsString(tuple3._2()._1());
        output[5] = objectMapper.writeValueAsString(tuple3._2()._2());
        rows.add(output);
      }
    }

    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_RECORD_COUNT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BLOCK_TYPE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_HEADER_METADATA)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FOOTER_METADATA);

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @ShellMethod(key = "show logfile records", value = "Read records from log files")
  public String showLogFileRecords(
      @ShellOption(value = {"--limit"}, help = "Limit number of records",
          defaultValue = "10") final Integer limit,
      @ShellOption(value = "--logFilePathPattern",
          help = "Fully qualified paths for the log files") final String logFilePathPattern,
      @ShellOption(value = "--mergeRecords", help = "If the records in the log files should be merged",
              defaultValue = "false") final Boolean shouldMerge)
      throws IOException {

    System.out.println("===============> Showing only " + limit + " records <===============");

    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    HoodieStorage storage = client.getStorage();
    List<String> logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(
        storage, new StoragePath(logFilePathPattern)).stream()
        .map(status -> status.getPath().toString()).sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());

    // logFilePaths size must > 1
    checkArgument(logFilePaths.size() > 0, "There is no log file");

    // TODO : readerSchema can change across blocks/log files, fix this inside Scanner
    Schema readerSchema = null;
    // get schema from last log file
    for (int i = logFilePaths.size() - 1; i >= 0; i--) {
      Schema schema = TableSchemaResolver.readSchemaFromLogFile(
          storage, new StoragePath(logFilePaths.get(i)));
      if (schema != null) {
        readerSchema = schema;
        break;
      }
    }
    Objects.requireNonNull(readerSchema);
    List<IndexedRecord> allRecords = new ArrayList<>();
    if (shouldMerge) {
      System.out.println("===========================> MERGING RECORDS <===================");
      HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
          HoodieCLI.getTableMetaClient().getStorage().getConf(),
          HoodieCLI.getTableMetaClient().getTableConfig(),
          Option.empty(),
          Option.empty());
      StoragePath firstLogFile = new StoragePath(logFilePaths.get(0));
      HoodieFileGroupId fileGroupId = new HoodieFileGroupId(FSUtils.getRelativePartitionPath(HoodieCLI.getTableMetaClient().getBasePath(), firstLogFile), FSUtils.getFileIdFromLogPath(firstLogFile));
      FileSlice fileSlice = new FileSlice(fileGroupId, HoodieTimeline.INIT_INSTANT_TS, null, logFilePaths.stream()
          .map(l -> new HoodieLogFile(new StoragePath(l))).collect(Collectors.toList()));
      TypedProperties fileGroupReaderProperties = buildFileGroupReaderProperties();
      try (HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(HoodieCLI.getTableMetaClient())
          .withFileSlice(fileSlice)
          .withDataSchema(readerSchema)
          .withRequestedSchema(readerSchema)
          .withLatestCommitTime(client.getActiveTimeline().getCommitAndReplaceTimeline().lastInstant().map(HoodieInstant::requestedTime).orElse(HoodieInstantTimeGenerator.getCurrentInstantTimeStr()))
          .withProps(fileGroupReaderProperties)
          .withShouldUseRecordPosition(false)
          .build();
           ClosableIterator<IndexedRecord> recordIterator = fileGroupReader.getClosableIterator()) {
        recordIterator.forEachRemaining(record -> {
          if (allRecords.size() < limit) {
            allRecords.add(record);
          }
        });
      }
    } else {
      for (String logFile : logFilePaths) {
        Schema writerSchema = TableSchemaResolver.readSchemaFromLogFile(
            client.getStorage(), new StoragePath(logFile));
        try (HoodieLogFormat.Reader reader =
                 HoodieLogFormat.newReader(storage, new HoodieLogFile(new StoragePath(logFile)), writerSchema)) {
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
        }
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

  /**
   * Derive necessary properties for FG reader.
   */
  private TypedProperties buildFileGroupReaderProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(
        MAX_MEMORY_FOR_MERGE.key(),
        Long.toString(MAX_MEMORY_FOR_MERGE.defaultValue()));
    props.setProperty(
        SPILLABLE_MAP_BASE_PATH.key(),
        FileIOUtils.getDefaultSpillableMapBasePath());
    props.setProperty(
        SPILLABLE_DISK_MAP_TYPE.key(),
        SPILLABLE_DISK_MAP_TYPE.defaultValue().name());
    props.setProperty(
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        Boolean.toString(DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()));

    return props;
  }
}
