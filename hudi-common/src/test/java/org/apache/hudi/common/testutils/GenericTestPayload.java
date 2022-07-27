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
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Generic class for specific payload implementations to inherit from.
 */
public abstract class GenericTestPayload {

  protected static final transient ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected String partitionPath;
  protected String rowKey;
  protected byte[] jsonDataCompressed;
  protected int dataSize;
  protected boolean isDeleted;
  protected Comparable orderingVal;

  public GenericTestPayload(Option<String> jsonData, String rowKey, String partitionPath, String schemaStr,
                            Boolean isDeleted, Comparable orderingVal) throws IOException {
    if (jsonData.isPresent()) {
      this.jsonDataCompressed = compressData(jsonData.get());
      this.dataSize = jsonData.get().length();
    }
    this.rowKey = rowKey;
    this.partitionPath = partitionPath;
    this.isDeleted = isDeleted;
    this.orderingVal = orderingVal;
  }

  public GenericTestPayload(String jsonData, String rowKey, String partitionPath, String schemaStr) throws IOException {
    this(Option.of(jsonData), rowKey, partitionPath, schemaStr, false, 0L);
  }

  public GenericTestPayload(String jsonData) throws IOException {
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

  public IndexedRecord getRecordToInsert(Schema schema) throws IOException {
    MercifulJsonConverter jsonConverter = new MercifulJsonConverter();
    return jsonConverter.convert(getJsonData(), schema);
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
}
