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
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieRecordLevelIndexMergedLogRecordScanner;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Utils for RecordKey Index.
 */
public class RecordLevelIndexUtils {

  private static final Logger LOG = LogManager.getLogger(RecordLevelIndexUtils.class);

  static final String SOLO_COMMIT_TIMESTAMP = "0000000000000";

  static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  static final int BUFFER_SIZE = 10 * 1024 * 1024;

  static Tuple2<HoodieFileReader<GenericRecord>, HoodieRecordLevelIndexMergedLogRecordScanner> openFileSlice(HoodieTableMetaClient datasetMetaClient, HoodieTableMetaClient indexMetaClient,
      SerializableConfiguration serializableConfiguration, String spillableMapBaseDir, String indexBasePath, int partitionIndex) throws IOException {

    HoodieFileReader<GenericRecord> baseFileReader = null;
    HoodieRecordLevelIndexMergedLogRecordScanner logRecordScanner = null;

    // String partitionBasePath = indexBasePath + Path.SEPARATOR + Integer.toString(partitionIndex);

    // HoodieTimer timer = new HoodieTimer().startTimer();
    String latestInstantTime = getLatestDatasetInstantTime(datasetMetaClient);

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
            spillableMapBaseDir, null);

        LOG.info("Opened index log files from " + logFilePaths + " at instant " + latestInstantTime
            + "(dataset instant=" + latestInstantTime + ", index instant=" + latestMetaInstantTimestamp + ")");
      }
    }
    return new Tuple2<>(baseFileReader, logRecordScanner);
  }

  static String getLatestDatasetInstantTime(HoodieTableMetaClient datasetMetaClient) {
    if (datasetMetaClient.getActiveTimeline().filterCompletedInstants().empty()) {
      return null;
    } else {
      return datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
          .map(HoodieInstant::getTimestamp).get();
    }
  }

  static void close(HoodieFileReader<GenericRecord> baseFileReader, HoodieRecordLevelIndexMergedLogRecordScanner logRecordScanner) {
    try {
      if (baseFileReader != null) {
        baseFileReader.close();
        baseFileReader = null;
      }
      if (logRecordScanner != null) {
        logRecordScanner.close();
        logRecordScanner = null;
      }
    } catch (Exception e) {
      throw new HoodieException("Error closing resources during metadata table merge", e);
    }
  }

}
