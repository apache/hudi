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

package org.apache.hudi.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieFlinkWriteableTestTable extends HoodieWriteableTestTable {
  private static final Logger LOG = LogManager.getLogger(HoodieFlinkWriteableTestTable.class);

  private HoodieFlinkWriteableTestTable(String basePath, org.apache.hadoop.fs.FileSystem fs, HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    super(basePath, fs, metaClient, schema, filter);
  }

  public static HoodieFlinkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    return new HoodieFlinkWriteableTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient, schema, filter);
  }

  public static HoodieFlinkWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema) {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    return of(metaClient, schema, filter);
  }

  public static HoodieFlinkWriteableTestTable of(HoodieTable hoodieTable, Schema schema) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema);
  }

  public static HoodieFlinkWriteableTestTable of(HoodieTable hoodieTable, Schema schema, BloomFilter filter) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema, filter);
  }

  @Override
  public HoodieFlinkWriteableTestTable addCommit(String instantTime) throws Exception {
    return (HoodieFlinkWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public HoodieFlinkWriteableTestTable forCommit(String instantTime) {
    return (HoodieFlinkWriteableTestTable) super.forCommit(instantTime);
  }

  public String getFileIdWithInserts(String partition) throws Exception {
    return getFileIdWithInserts(partition, new HoodieRecord[0]);
  }

  public String getFileIdWithInserts(String partition, HoodieRecord... records) throws Exception {
    return getFileIdWithInserts(partition, Arrays.asList(records));
  }

  public String getFileIdWithInserts(String partition, List<HoodieRecord> records) throws Exception {
    String fileId = java.util.UUID.randomUUID().toString();
    withInserts(partition, fileId, records);
    return fileId;
  }

  public HoodieFlinkWriteableTestTable withInserts(String partition, String fileId) throws Exception {
    return withInserts(partition, fileId, new HoodieRecord[0]);
  }

  public HoodieFlinkWriteableTestTable withInserts(String partition, String fileId, HoodieRecord... records) throws Exception {
    return withInserts(partition, fileId, Arrays.asList(records));
  }

  public HoodieFlinkWriteableTestTable withInserts(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    return (HoodieFlinkWriteableTestTable) withInserts(partition, fileId, records, new org.apache.hudi.client.FlinkTaskContextSupplier(null));
  }

  public Map<String, List<HoodieLogFile>> withLogAppends(List<HoodieRecord> records) throws Exception {
    Map<String, List<HoodieLogFile>> partitionToLogfilesMap = new HashMap<>();
    for (List<HoodieRecord> groupedRecords : records.stream().collect(
        Collectors.groupingBy(HoodieRecord::getCurrentLocation)).values()) {
      final Pair<String, HoodieLogFile> appendedLogFile = appendRecordsToLogFile(groupedRecords);
      partitionToLogfilesMap.computeIfAbsent(
          appendedLogFile.getKey(), k -> new ArrayList<>()).add(appendedLogFile.getValue());
    }
    return partitionToLogfilesMap;
  }

  private Pair<String, HoodieLogFile> appendRecordsToLogFile(List<HoodieRecord> groupedRecords) throws Exception {
    String partitionPath = groupedRecords.get(0).getPartitionPath();
    HoodieRecordLocation location = groupedRecords.get(0).getCurrentLocation();
    try (HoodieLogFormat.Writer logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(basePath, partitionPath))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
        .overBaseCommit(location.getInstantTime()).withFs(fs).build()) {
      Map<HeaderMetadataType, String> header = new java.util.HashMap<>();
      header.put(HeaderMetadataType.INSTANT_TIME, location.getInstantTime());
      header.put(HeaderMetadataType.SCHEMA, schema.toString());
      logWriter.appendBlock(new HoodieAvroDataBlock(groupedRecords.stream().map(r -> {
        try {
          GenericRecord val = (GenericRecord) r.getData().getInsertValue(schema).get();
          HoodieAvroUtils.addHoodieKeyToRecord(val, r.getRecordKey(), r.getPartitionPath(), "");
          return (org.apache.avro.generic.IndexedRecord) val;
        } catch (java.io.IOException e) {
          LOG.warn("Failed to convert record " + r.toString(), e);
          return null;
        }
      }).collect(Collectors.toList()), header));
      return Pair.of(partitionPath, logWriter.getLogFile());
    }
  }
}
