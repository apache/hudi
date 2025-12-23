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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.util.FileIOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.extractPartitionFromTimeField;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Example row change event based on some example data used by testcases. The data avro schema is
 * src/test/resources/schema1.
 */
public class RawTripTestPayload implements HoodieRecordPayload<RawTripTestPayload> {
  private static final MercifulJsonConverter JSON_CONVERTER = new MercifulJsonConverter();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  @Getter
  private String partitionPath;
  @Getter
  private String rowKey;
  private byte[] jsonDataCompressed;
  private int dataSize;
  private boolean isDeleted;
  private Comparable orderingVal;

  public RawTripTestPayload(String jsonData) throws IOException {
    this.jsonDataCompressed = compressData(jsonData);
    this.dataSize = jsonData.length();
    Map<String, Object> jsonRecordMap = OBJECT_MAPPER.readValue(jsonData, Map.class);
    this.rowKey = jsonRecordMap.get("_row_key").toString();
    this.partitionPath = extractPartitionFromTimeField(jsonRecordMap.get("time").toString());
    this.isDeleted = false;
    this.orderingVal = Integer.valueOf(jsonRecordMap.getOrDefault("number", 0).toString());
  }

  @Override
  public RawTripTestPayload preCombine(RawTripTestPayload oldValue) {
    if (!orderingVal.equals(DEFAULT_ORDERING_VALUE) && oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
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
      return Option.of(JSON_CONVERTER.convert(getJsonData(), schema));
    }
  }

  @Override
  public Comparable<?> getOrderingValue() {
    return orderingVal;
  }

  public IndexedRecord getRecordToInsert(Schema schema) throws IOException {
    return JSON_CONVERTER.convert(getJsonData(), schema);
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    // Let's assume we want to count the number of input row change events
    // that are processed. Let the time-bucket for this row change event be 1506582000.
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put("InputRecordCount_1506582000", "2");
    return Option.of(metadataMap);
  }

  public String getJsonData() throws IOException {
    return unCompressData(jsonDataCompressed);
  }

  private byte[] compressData(String jsonData) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION), true);
    try {
      dos.write(getUTF8Bytes(jsonData));
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
}
