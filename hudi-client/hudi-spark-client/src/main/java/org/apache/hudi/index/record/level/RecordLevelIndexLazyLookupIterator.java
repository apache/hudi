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
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieRecordLevelIndexMergedLogRecordScanner;
import org.apache.hudi.index.HoodieRecordLevelIndexPayload;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

import scala.Tuple2;

public class RecordLevelIndexLazyLookupIterator<T extends HoodieRecordPayload>
    extends LazyIterableIterator<Tuple2<HoodieKey, HoodieRecord<T>>, Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>> {

  private static final Logger LOG = LogManager.getLogger(RecordLevelIndexLazyLookupIterator.class);

  private HoodieWriteConfig datasetWriteConfig;
  private HoodieTableMetaClient datasetMetaClient;
  private HoodieTableMetaClient indexMetaClient;
  private SerializableConfiguration serializableConfiguration;

  private Schema schema;
  private int partitionIndex;
  private String indexBasePath;

  // Readers for the base and log file which store the index
  private HoodieFileReader<GenericRecord> baseFileReader;
  private HoodieRecordLevelIndexMergedLogRecordScanner logRecordScanner;

  RecordLevelIndexLazyLookupIterator(Iterator<Tuple2<HoodieKey, HoodieRecord<T>>> recordItr,
      HoodieWriteConfig writeConfig, HoodieWriteConfig recordLevelIndexWriteConfig,
      HoodieTableMetaClient datasetMetaClient, HoodieTableMetaClient indexMetaClient, SerializableConfiguration serializableConfiguration,
      int partitionIndex) {
    super(recordItr);
    this.datasetWriteConfig = writeConfig;
    this.datasetMetaClient = datasetMetaClient;
    this.indexMetaClient = indexMetaClient;
    this.serializableConfiguration = serializableConfiguration;
    this.indexBasePath = recordLevelIndexWriteConfig.getBasePath();
    this.partitionIndex = partitionIndex;
    schema = HoodieAvroUtils.addMetadataFields(HoodieRecordLevelIndexRecord.getClassSchema());
  }

  @Override
  protected void start() {
    try {
      Tuple2<HoodieFileReader<GenericRecord>, HoodieRecordLevelIndexMergedLogRecordScanner> baseLogFileReaders =
          RecordLevelIndexUtils.openFileSlice(datasetMetaClient, indexMetaClient, serializableConfiguration, datasetWriteConfig.getSpillableMapBasePath(), indexBasePath, partitionIndex);
      baseFileReader = baseLogFileReaders._1;
      logRecordScanner = baseLogFileReaders._2;
    } catch (IOException e) {
      e.printStackTrace();
      throw new HoodieException("Exception thrown while trying to load index partition files", e);
    }
  }

  @Override
  protected Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>> computeNext() {

    if (inputItr.hasNext()) {
      Tuple2<HoodieKey, HoodieRecord<T>> recordToLookup = inputItr.next();
      String keyToLookup = recordToLookup._1.getRecordKey() + "_" + recordToLookup._1.getPartitionPath();
      HoodieRecord<HoodieRecordLevelIndexPayload> hoodieRecord = null;
      try {
        if (baseFileReader != null) {
          Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(keyToLookup);
          if (baseRecord.isPresent()) {
            hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
                HoodieRecordLevelIndexPayload.class.getName());
          }
        }

        if (logRecordScanner != null) {
          Option<HoodieRecord<HoodieRecordLevelIndexPayload>> logRecord = logRecordScanner.getRecordByKey(keyToLookup);
          if (logRecord.isPresent()) {
            if (hoodieRecord != null) { // both log record and base record is present
              HoodieRecordPayload mergedPayload = logRecord.get().getData().preCombine(hoodieRecord.getData());
              Option<HoodieRecordLevelIndexRecord> mergedRecord = mergedPayload.getInsertValue(schema);
              if (mergedRecord.isPresent()) { // both baseRecord and log record is present
                return new Tuple2<>(recordToLookup._1, Option.of(new Tuple2<>(mergedRecord.get().getPartitionPath(),
                    new HoodieRecordLocation(mergedRecord.get().getInstantTime(), mergedRecord.get().getFileId()))));
              } else {
                // not sure what needs to be done here.
              }
            } else { // only log record
              HoodieRecordLevelIndexPayload logPayload = logRecord.get().getData();
              return new Tuple2<>(recordToLookup._1, Option.of(new Tuple2<>(logPayload.getPartitionPath(),
                  new HoodieRecordLocation(logPayload.getInstantTime(), logPayload.getFileId()))));
            }
          } else { // no log record
            // no op
          }
        }
        if (hoodieRecord != null) { // only base record
          HoodieRecordLevelIndexPayload basePayload = hoodieRecord.getData();
          return new Tuple2<>(recordToLookup._1, Option.of(new Tuple2<>(basePayload.getPartitionPath(),
              new HoodieRecordLocation(basePayload.getInstantTime(), basePayload.getFileId()))));
        } else { // not present in both
          return new Tuple2<>(recordToLookup._1, Option.empty());
        }
      } catch (Exception e) {
        throw new HoodieException("Index look up failed w/ exception ", e);
      }
    }
    return null;
  }

  @Override
  protected void end() {
    RecordLevelIndexUtils.close(baseFileReader, logRecordScanner);
  }

  /**
   * Open readers to the base and log files.
   */
  /*private void openFileSlice(int partitionIndex) throws IOException {
    // String partitionBasePath = indexBasePath + Path.SEPARATOR + Integer.toString(partitionIndex);

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
  }*/
}

