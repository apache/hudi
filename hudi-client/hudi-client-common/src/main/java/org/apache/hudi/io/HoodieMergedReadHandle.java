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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
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
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class HoodieMergedReadHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  protected final Schema readerSchema;
  protected final Schema baseFileReaderSchema;
  private final Option<FileSlice> fileSliceOpt;

  public HoodieMergedReadHandle(HoodieWriteConfig config,
                                Option<String> instantTime,
                                HoodieTable<T, I, K, O> hoodieTable,
                                Pair<String, String> partitionPathFileIDPair) {
    this(config, instantTime, hoodieTable, partitionPathFileIDPair, Option.empty());
  }

  public HoodieMergedReadHandle(HoodieWriteConfig config,
                                Option<String> instantTime,
                                HoodieTable<T, I, K, O> hoodieTable,
                                Pair<String, String> partitionPathFileIDPair,
                                Option<FileSlice> fileSliceOption) {
    super(config, instantTime, hoodieTable, partitionPathFileIDPair);
    readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    // config.getSchema is not canonicalized, while config.getWriteSchema is canonicalized. So, we have to use the canonicalized schema to read the existing data.
    baseFileReaderSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getWriteSchema()), config.allowOperationMetadataField());
    fileSliceOpt = fileSliceOption.isPresent() ? fileSliceOption : getLatestFileSlice();
  }

  public List<HoodieRecord<T>> getMergedRecords() {
    if (!fileSliceOpt.isPresent()) {
      return Collections.emptyList();
    }
    checkState(nonEmpty(instantTime), String.format("Expected a valid instant time but got `%s`", instantTime));
    final FileSlice fileSlice = fileSliceOpt.get();
    String baseFileInstantTime = fileSlice.getBaseFile().get().getCommitTime();
    final HoodieRecordLocation currentLocation = new HoodieRecordLocation(baseFileInstantTime, fileSlice.getFileId());
    Option<HoodieFileReader> baseFileReader = Option.empty();
    HoodieMergedLogRecordScanner logRecordScanner = null;
    try {
      baseFileReader = getBaseFileReader(fileSlice);
      logRecordScanner = getLogRecordScanner(fileSlice);
      List<HoodieRecord<T>> mergedRecords = new ArrayList<>();
      doMergedRead(baseFileReader, logRecordScanner).forEach(r -> {
        r.unseal();
        r.setCurrentLocation(currentLocation);
        r.seal();
        mergedRecords.add(r);
      });
      return mergedRecords;
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
  }

  private Option<FileSlice> getLatestFileSlice() {
    if (nonEmpty(instantTime)
        && hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant().isPresent()) {
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
    List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
        .map(l -> l.getPath().toString()).collect(toList());
    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(hoodieTable.getMetaClient().getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(IOUtils.getMaxMemoryPerCompaction(hoodieTable.getTaskContextSupplier(), config))
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(fileSlice.getPartitionPath())
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .withTableMetaClient(hoodieTable.getMetaClient())
        .build();
  }

  private List<HoodieRecord<T>> doMergedRead(Option<HoodieFileReader> baseFileReaderOpt, HoodieMergedLogRecordScanner logRecordScanner) throws IOException {
    List<HoodieRecord<T>> mergedRecords = new ArrayList<>();
    Map<String, HoodieRecord> deltaRecordMap = logRecordScanner.getRecords();
    Set<String> deltaRecordKeys = new HashSet<>(deltaRecordMap.keySet());

    if (baseFileReaderOpt.isPresent()) {
      HoodieFileReader baseFileReader = baseFileReaderOpt.get();
      HoodieRecordMerger recordMerger = config.getRecordMerger();
      ClosableIterator<HoodieRecord<T>> baseFileItr = baseFileReader.getRecordIterator(baseFileReaderSchema);
      HoodieTableConfig tableConfig = hoodieTable.getMetaClient().getTableConfig();
      Option<Pair<String, String>> simpleKeyGenFieldsOpt =
          tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
      while (baseFileItr.hasNext()) {
        HoodieRecord<T> record = baseFileItr.next().wrapIntoHoodieRecordPayloadWithParams(readerSchema,
            config.getProps(), simpleKeyGenFieldsOpt, logRecordScanner.isWithOperationField(), logRecordScanner.getPartitionNameOverride(), false, Option.empty());
        String key = record.getRecordKey();
        if (deltaRecordMap.containsKey(key)) {
          deltaRecordKeys.remove(key);
          Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger
              .merge(record, readerSchema, deltaRecordMap.get(key), readerSchema, config.getPayloadConfig().getProps());
          if (!mergeResult.isPresent()) {
            continue;
          }
          HoodieRecord<T> r = mergeResult.get().getLeft().wrapIntoHoodieRecordPayloadWithParams(readerSchema,
              config.getProps(), simpleKeyGenFieldsOpt, logRecordScanner.isWithOperationField(), logRecordScanner.getPartitionNameOverride(), false, Option.empty());
          mergedRecords.add(r);
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

