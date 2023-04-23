/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class HoodieMergedReadHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {
  public HoodieMergedReadHandle(HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      Pair<String, String> partitionPathFileIDPair) {
    super(config, Option.of(instantTime), hoodieTable, partitionPathFileIDPair);
  }

  public List<HoodieRecord<T>> getMergedRecords() {
    Option<FileSlice> fileSliceOpt = getLatestFileSlice();
    if (!fileSliceOpt.isPresent()) {
      return Collections.emptyList();
    }
    FileSlice fileSlice = fileSliceOpt.get();
    Option<HoodieFileReader> baseFileReader = Option.empty();
    HoodieMergedLogRecordScanner logRecordScanner = null;
    List<HoodieRecord<T>> records = new ArrayList<>();
    try {
      baseFileReader = getBaseFileReader(fileSlice);
      logRecordScanner = getLogRecordScanner(fileSlice);
      HoodieRecordLocation currentLocation = new HoodieRecordLocation(instantTime, fileSlice.getFileId());
      doMergedRead(baseFileReader, logRecordScanner).forEach(r -> {
        r.unseal();
        r.setCurrentLocation(currentLocation);
        r.seal();
        records.add(r);
      });
    } catch (IOException e) {
      throw new HoodieIndexException("Error in reading " + fileSlice, e);
    } finally {
      if (baseFileReader.isPresent()) {
        baseFileReader.get().close();
      }
      if (logRecordScanner != null) {
        logRecordScanner.close();
      }
    }
    return records;
  }

  private Option<FileSlice> getLatestFileSlice() {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return Option.fromJavaOptional(hoodieTable
          .getHoodieView()
          .getLatestMergedFileSlicesBeforeOrOn(partitionPathFileIDPair.getLeft(), instantTime)
          .filter(fileSlice -> fileSlice.getFileId().equals(partitionPathFileIDPair.getRight()))
          .findFirst());
    }
    return Option.empty();
  }

  private Option<HoodieFileReader> getBaseFileReader(FileSlice fileSlice) throws IOException {
    if (fileSlice.getBaseFile().isPresent()) {
      return Option.of(createNewFileReader(fileSlice.getBaseFile().get()));
    }
    return Option.empty();
  }

  private HoodieMergedLogRecordScanner getLogRecordScanner(FileSlice fileSlice) {
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(hoodieTable.getTaskContextSupplier(), config);
    List<String> logFilePaths = fileSlice.getLogFiles().map(l -> l.getPath().toString()).collect(toList());
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(hoodieTable.getMetaClient().getFs())
        .withBasePath(hoodieTable.getMetaClient().getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(fileSlice.getPartitionPath())
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .build();
  }

  private List<HoodieRecord<T>> doMergedRead(Option<HoodieFileReader> baseFileReaderOpt, HoodieMergedLogRecordScanner scanner) throws IOException {
    List<HoodieRecord<T>> mergedRecords = new ArrayList<>();
    Map<String, HoodieRecord> deltaRecordMap = scanner.getRecords();
    Set<String> deltaRecordKeys = new HashSet<>(deltaRecordMap.keySet());

    if (baseFileReaderOpt.isPresent()) {
      HoodieFileReader baseFileReader = baseFileReaderOpt.get();
      HoodieRecordMerger recordMerger = config.getRecordMerger();
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
      Schema writeSchema = new Schema.Parser().parse(config.getWriteSchema());
      Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());

      ClosableIterator<HoodieRecord<T>> baseFileItr = baseFileReader.getRecordIterator(readerSchema);
      HoodieTableConfig tableConfig = hoodieTable.getMetaClient().getTableConfig();
      Option<Pair<String, String>> simpleKeyGenFieldsOpt =
          tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
      while (baseFileItr.hasNext()) {
        HoodieRecord<T> record = baseFileItr.next().wrapIntoHoodieRecordPayloadWithParams(readerSchema,
            tableConfig.getProps(), simpleKeyGenFieldsOpt, scanner.isWithOperationField(), scanner.getPartitionNameOverride(), false);
        String key = record.getRecordKey();
        if (deltaRecordMap.containsKey(key)) {
          deltaRecordKeys.remove(key);
          Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(record.copy(), writeSchemaWithMetaFields, deltaRecordMap.get(key), writeSchemaWithMetaFields, config.getProps());
          if (!mergeResult.isPresent()) {
            continue;
          }
          mergedRecords.add(mergeResult.get().getLeft());
        } else {
          mergedRecords.add(record.copy());
        }
      }
    }

    for (String key : deltaRecordKeys) {
      mergedRecords.add(deltaRecordMap.get(key));
    }

    return mergedRecords;
  }
}

