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

package org.apache.hudi.index.record.level;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieRecordLevelIndexRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieRecordLevelIndexMergedLogRecordScanner;
import org.apache.hudi.index.HoodieRecordLevelIndexPayload;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import scala.Tuple2;

public class HoodieRecordLevelIndexScanner implements Serializable {

  // Table name suffix
  private static final String RECORD_LEVEL_INDEX_TABLE_NAME_SUFFIX = "index";

  // Base path of the RecordLevelIndex Table relative to the dataset (.hoodie/index)
  private static final String RECORD_LEVEL_INDEX_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + RECORD_LEVEL_INDEX_TABLE_NAME_SUFFIX;

  private static final Logger LOG = LogManager.getLogger(HoodieRecordLevelIndexScanner.class);

  private HoodieWriteConfig datasetWriteConfig;
  private HoodieTableMetaClient datasetMetaClient;
  private HoodieTableMetaClient indexMetaClient;
  private SerializableConfiguration serializableConfiguration;

  private String indexBasePath;

  // Readers for the base and log file which store the metadata
  private HoodieFileReader<GenericRecord> baseFileReader;
  private HoodieRecordLevelIndexMergedLogRecordScanner logRecordScanner;

  private List<HoodieKey> recordKeys;
  private int partitionIndex;

  public HoodieRecordLevelIndexScanner(HoodieWriteConfig writeConfig, HoodieWriteConfig recordLevelIndexWriteConfig,
      HoodieTableMetaClient datasetMetaClient, String indexTableName, SerializableConfiguration serializableConfiguration,
      List<HoodieKey> recordKeys, int partitionIndex) throws IOException {
    this.datasetWriteConfig = writeConfig;
    this.datasetMetaClient = datasetMetaClient;
    this.indexMetaClient = HoodieTableMetaClient.initTableType(serializableConfiguration.get(), recordLevelIndexWriteConfig.getBasePath(),
        HoodieTableType.MERGE_ON_READ, indexTableName, "archived", HoodieRecordLevelIndexRecord.class.getName(),
        HoodieFileFormat.HFILE.toString());
    this.serializableConfiguration = serializableConfiguration;
    this.indexBasePath = recordLevelIndexWriteConfig.getBasePath();
    this.recordKeys = recordKeys;
    this.partitionIndex = partitionIndex;
  }

  public List<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>> getRecordLocations() {

    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieRecordLevelIndexRecord.getClassSchema());

    try {
      List<Long> timings = new ArrayList<>();
      HoodieTimer timer = new HoodieTimer().startTimer();
      Tuple2<HoodieFileReader<GenericRecord>, HoodieRecordLevelIndexMergedLogRecordScanner> baseLogFileReaders =
          RecordLevelIndexUtils.openFileSlice(datasetMetaClient, indexMetaClient, serializableConfiguration, datasetWriteConfig.getSpillableMapBasePath(), indexBasePath, partitionIndex);
      baseFileReader = baseLogFileReaders._1;
      logRecordScanner = baseLogFileReaders._2;

      timings.add(timer.endTimer());

      timer.startTimer();

      List<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>> toReturn = new ArrayList<>();
      List<String> keysToLookup = new ArrayList<>();
      Map<String, HoodieKey> keyStringToHoodieKeyMap = new HashMap<>();
      Set<String> matchedKeys = new HashSet<>();
      // store map from key string to HoodieKey
      recordKeys.forEach(entry -> {
        keysToLookup.add(entry.getRecordKey() + "_" + entry.getPartitionPath());
        keyStringToHoodieKeyMap.put(entry.getRecordKey() + "_" + entry.getPartitionPath(), entry);
      });
      // sort input keys since we are planning to scan base file and lookup as we go.
      // we could avoid sort here since we do repartition and sort within partitions in the driver before calling into this.
      Collections.sort(keysToLookup);

      int curIndex = 0;
      int totalSize = keysToLookup.size();

      // Retrieve records from base file
      Map<String, HoodieRecord<HoodieRecordLevelIndexPayload>> hoodieRecords = new HashMap<>();
      if (baseFileReader != null) {
        Iterator<GenericRecord> genericRecordIterator = baseFileReader.getRecordIterator();
        // iterate all records from index base file and find matching records
        while (genericRecordIterator.hasNext()) {
          GenericRecord genericRecord = genericRecordIterator.next();
          if (keysToLookup.get(curIndex).equals(genericRecord.get("key"))) {
            hoodieRecords.put(keysToLookup.get(curIndex), SpillableMapUtils.convertToHoodieRecordPayload(genericRecord,
                HoodieRecordLevelIndexPayload.class.getName()));
            matchedKeys.add(keysToLookup.get(curIndex));
            curIndex++;
            if (curIndex >= totalSize) {
              break;
            }
          }
        }
      }
      timings.add(timer.endTimer());

      // Retrieve record from log file
      timer.startTimer();
      if (logRecordScanner != null) {
        Map<String, Option<HoodieRecord<HoodieRecordLevelIndexPayload>>> logHoodieRecords = logRecordScanner.getRecordsByKey(keysToLookup);
        for (Map.Entry<String, Option<HoodieRecord<HoodieRecordLevelIndexPayload>>> entry : logHoodieRecords.entrySet()) {
          String hoodieKeyStr = entry.getKey();
          if (hoodieRecords.containsKey(hoodieKeyStr)) {
            if (entry.getValue().isPresent()) { // record present in both base file and log file
              HoodieRecordPayload mergedPayload = entry.getValue().get().getData().preCombine(hoodieRecords.get(hoodieKeyStr).getData());
              Option<HoodieRecordLevelIndexRecord> mergedRecord = mergedPayload.getInsertValue(schema);
              if (mergedRecord.isPresent()) {
                toReturn.add(new Tuple2<>(keyStringToHoodieKeyMap.get(hoodieKeyStr), Option.of(new Tuple2<>(mergedRecord.get().getPartitionPath(),
                    new HoodieRecordLocation(mergedRecord.get().getInstantTime(), mergedRecord.get().getFileId())))));
              } else {
                // do we need to do anything here? is this reachable?
              }
            } else { // record present only in base file
              HoodieRecord<HoodieRecordLevelIndexPayload> baseRecord = hoodieRecords.get(hoodieKeyStr);
              HoodieRecordLevelIndexPayload hoodieRecordLevelIndexPayload = baseRecord.getData();
              toReturn.add(new Tuple2<>(keyStringToHoodieKeyMap.get(hoodieKeyStr), Option.of(new Tuple2<>(hoodieRecordLevelIndexPayload.getPartitionPath(),
                  new HoodieRecordLocation(hoodieRecordLevelIndexPayload.getInstantTime(), hoodieRecordLevelIndexPayload.getFileId())))));
            }
          } else if (entry.getValue().isPresent()) { // record present only in log file
            matchedKeys.add(hoodieKeyStr);
            Option<HoodieRecord<HoodieRecordLevelIndexPayload>> logRecordOpt = entry.getValue();
            if (logRecordOpt.isPresent()) {
              HoodieRecordLevelIndexPayload logRecordPayload = logRecordOpt.get().getData();
              toReturn.add(new Tuple2<>(keyStringToHoodieKeyMap.get(hoodieKeyStr), Option.of(new Tuple2<>(logRecordPayload.getPartitionPath(),
                  new HoodieRecordLocation(logRecordPayload.getInstantTime(), logRecordPayload.getFileId())))));
            }
          }
        }
      } else {
        for (Entry<String, HoodieRecord<HoodieRecordLevelIndexPayload>> record : hoodieRecords.entrySet()) {
          HoodieRecordLevelIndexPayload hoodieRecordLevelIndexPayload = record.getValue().getData();
          toReturn.add(new Tuple2<>(keyStringToHoodieKeyMap.get(record.getKey()), Option.of(new Tuple2<>(hoodieRecordLevelIndexPayload.getPartitionPath(),
              new HoodieRecordLocation(hoodieRecordLevelIndexPayload.getInstantTime(), hoodieRecordLevelIndexPayload.getFileId())))));
        }
      }

      // unmatched keys
      for (String key : keysToLookup) {
        if (!matchedKeys.contains(key)) {
          toReturn.add(new Tuple2<>(keyStringToHoodieKeyMap.get(key), Option.empty()));
        }
      }

      timings.add(timer.endTimer());
      // LOG.info(String.format("Metadata read for key %s took [open, baseFileRead, logMerge] %s ms", key, timings));
      return toReturn;
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from index table for key :" + Arrays.toString(recordKeys.toArray()), ioe);
    } finally {
      RecordLevelIndexUtils.close(baseFileReader, logRecordScanner);
    }
  }

  /**
   * Open readers to the base and log files.
   */
  /*private void openFileSlice(int partitionIndex) throws IOException {

    // String partitionBasePath = indexBasePath + Path.SEPARATOR + Integer.toString(partitionIndex);

    // HoodieTimer timer = new HoodieTimer().startTimer();
    String latestInstantTime = getLatestDatasetInstantTime();

    if (latestInstantTime != null) {
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(indexMetaClient, indexMetaClient.getActiveTimeline().filterCompletedInstants());
      List<FileSlice> latestFileSystemMetadataSlices = fsView.getLatestFileSlices(Integer.toString(partitionIndex)).collect(Collectors.toList());

      // If the base file is present then create a reader
      Option<HoodieBaseFile> basefile = !latestFileSystemMetadataSlices.isEmpty() ? latestFileSystemMetadataSlices.get(0).getBaseFile() : Option.empty();
      if (basefile.isPresent()) {
        String basefilePath = basefile.get().getPath();
        baseFileReader = HoodieFileReaderFactory.getFileReader(serializableConfiguration.get(), new Path(basefilePath));
        LOG.info("Opened index base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
      }

      // Open the log record scanner using the log files from the latest file slice
      List<String> logFilePaths = !latestFileSystemMetadataSlices.isEmpty() ? latestFileSystemMetadataSlices.get(0).getLogFiles()
          .sorted(HoodieLogFile.getLogFileComparator())
          .map(o -> o.getPath().toString())
          .collect(Collectors.toList()) : Collections.emptyList();
      Option<HoodieInstant> lastInstant = indexMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
      String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

      // Load the schema
      Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
      if (!logFilePaths.isEmpty()) {
        logRecordScanner = new HoodieRecordLevelIndexMergedLogRecordScanner(indexMetaClient.getFs(), indexBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, MAX_MEMORY_SIZE_IN_BYTES, BUFFER_SIZE,
            datasetWriteConfig.getSpillableMapBasePath(), null);

        LOG.info("Opened index log files from " + logFilePaths + " at instant " + latestInstantTime
            + "(dataset instant=" + latestInstantTime + ", index instant=" + latestMetaInstantTimestamp + ")");
      }
    }
  }

  protected String getLatestDatasetInstantTime() {
    if (datasetMetaClient.getActiveTimeline().filterCompletedInstants().empty()) {
      return null;
    } else {
      return datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
          .map(HoodieInstant::getTimestamp).get();
    }
  } */

}
