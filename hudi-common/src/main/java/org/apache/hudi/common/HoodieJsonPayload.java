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

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Hoodie json payload.
 */
public class HoodieJsonPayload implements HoodieRecordPayload<HoodieJsonPayload> {

  private final byte[] jsonDataCompressed;
  private final int dataSize;

  public HoodieJsonPayload(String json) throws IOException {
    this.jsonDataCompressed = compressData(json);
    this.dataSize = json.length();
  }

  @Override
  public HoodieJsonPayload preCombine(HoodieJsonPayload oldValue) {
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRec, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    MercifulJsonConverter jsonConverter = new MercifulJsonConverter();
    return Option.of(jsonConverter.convert(getJsonData(), HoodieSchema.fromAvroSchema(schema)));
  }

  private String getJsonData() throws IOException {
    return unCompressData(jsonDataCompressed);
  }

  private byte[] compressData(String jsonData) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
    DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater, true);
    try {
      dos.write(getUTF8Bytes(jsonData));
    } finally {
      dos.flush();
      dos.close();
      // Its important to call this.
      // Deflater takes off-heap native memory and does not release until GC kicks in
      deflater.end();
    }
    return baos.toByteArray();
  }

  private String unCompressData(byte[] data) throws IOException {
    try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data))) {
      return FileIOUtils.readAsUTFString(iis, dataSize);
    }
  }

  private String getFieldFromJsonOrFail(String field) throws IOException {
    JsonNode node = new ObjectMapper().readTree(getJsonData());
    if (!node.has(field)) {
      throw new HoodieException("Field :" + field + " not found in payload => " + node);
    }
    return node.get(field).textValue();
  }

  public String getRowKey(String keyColumnField) throws IOException {
    return getFieldFromJsonOrFail(keyColumnField);
  }

  public String getPartitionPath(String partitionPathField) throws IOException {
    return getFieldFromJsonOrFail(partitionPathField);
  }
}
