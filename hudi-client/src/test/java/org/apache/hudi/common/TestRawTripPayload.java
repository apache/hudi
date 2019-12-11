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

package org.apache.hudi.common;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Example row change event based on some example data used by testcases. The data avro schema is
 * src/test/resources/schema1.
 */
public class TestRawTripPayload implements HoodieRecordPayload<TestRawTripPayload> {

  private static final transient ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String partitionPath;
  private String rowKey;
  private byte[] jsonDataCompressed;
  private int dataSize;
  private boolean isDeleted;

  public TestRawTripPayload(Option<String> jsonData, String rowKey, String partitionPath, String schemaStr,
      Boolean isDeleted) throws IOException {
    if (jsonData.isPresent()) {
      this.jsonDataCompressed = compressData(jsonData.get());
      this.dataSize = jsonData.get().length();
    }
    this.rowKey = rowKey;
    this.partitionPath = partitionPath;
    this.isDeleted = isDeleted;
  }

  public TestRawTripPayload(String jsonData, String rowKey, String partitionPath, String schemaStr) throws IOException {
    this(Option.of(jsonData), rowKey, partitionPath, schemaStr, false);
  }

  public TestRawTripPayload(String jsonData) throws IOException {
    this.jsonDataCompressed = compressData(jsonData);
    this.dataSize = jsonData.length();
    Map<String, Object> jsonRecordMap = OBJECT_MAPPER.readValue(jsonData, Map.class);
    this.rowKey = jsonRecordMap.get("_row_key").toString();
    this.partitionPath = jsonRecordMap.get("time").toString().split("T")[0].replace("-", "/");
    this.isDeleted = false;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  @Override
  public TestRawTripPayload preCombine(TestRawTripPayload another) {
    return another;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRec, Schema schema) throws IOException {
    return this.getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (isDeleted) {
      return Option.empty();
    } else {
      MercifulJsonConverter jsonConverter = new MercifulJsonConverter();
      return Option.of(jsonConverter.convert(getJsonData(), schema));
    }
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    // Let's assume we want to count the number of input row change events
    // that are processed. Let the time-bucket for this row change event be 1506582000.
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put("InputRecordCount_1506582000", "2");
    return Option.of(metadataMap);
  }

  public String getRowKey() {
    return rowKey;
  }

  public String getJsonData() throws IOException {
    return unCompressData(jsonDataCompressed);
  }

  private byte[] compressData(String jsonData) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION), true);
    try {
      dos.write(jsonData.getBytes());
    } finally {
      dos.flush();
      dos.close();
    }
    return baos.toByteArray();
  }

  private String unCompressData(byte[] data) throws IOException {
    try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data))) {
      return FileIOUtils.readAsUTFString(iis, dataSize);
    }
  }

  /**
   * A custom {@link WriteStatus} that merges passed metadata key value map to {@code WriteStatus.markSuccess()} and
   * {@code WriteStatus.markFailure()}.
   */
  public static class MetadataMergeWriteStatus extends WriteStatus {

    private Map<String, String> mergedMetadataMap = new HashMap<>();

    public MetadataMergeWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
      super(trackSuccessRecords, failureFraction);
    }

    public static Map<String, String> mergeMetadataForWriteStatuses(List<WriteStatus> writeStatuses) {
      Map<String, String> allWriteStatusMergedMetadataMap = new HashMap<>();
      for (WriteStatus writeStatus : writeStatuses) {
        MetadataMergeWriteStatus.mergeMetadataMaps(((MetadataMergeWriteStatus) writeStatus).getMergedMetadataMap(),
            allWriteStatusMergedMetadataMap);
      }
      return allWriteStatusMergedMetadataMap;
    }

    private static void mergeMetadataMaps(Map<String, String> mergeFromMap, Map<String, String> mergeToMap) {
      for (Entry<String, String> entry : mergeFromMap.entrySet()) {
        String key = entry.getKey();
        if (!mergeToMap.containsKey(key)) {
          mergeToMap.put(key, "0");
        }
        mergeToMap.put(key, addStrsAsInt(entry.getValue(), mergeToMap.get(key)));
      }
    }

    private static String addStrsAsInt(String a, String b) {
      return String.valueOf(Integer.parseInt(a) + Integer.parseInt(b));
    }

    @Override
    public void markSuccess(HoodieRecord record, Option<Map<String, String>> recordMetadata) {
      super.markSuccess(record, recordMetadata);
      if (recordMetadata.isPresent()) {
        mergeMetadataMaps(recordMetadata.get(), mergedMetadataMap);
      }
    }

    @Override
    public void markFailure(HoodieRecord record, Throwable t, Option<Map<String, String>> recordMetadata) {
      super.markFailure(record, t, recordMetadata);
      if (recordMetadata.isPresent()) {
        mergeMetadataMaps(recordMetadata.get(), mergedMetadataMap);
      }
    }

    private Map<String, String> getMergedMetadataMap() {
      return mergedMetadataMap;
    }
  }
}
