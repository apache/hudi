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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.avro.HoodieAvroUtils.createHoodieRecordFromAvro;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Example row change event based on some example data used by testcases. The data avro schema is
 * src/test/resources/schema1.
 */
public class RawTripTestPayload implements HoodieRecordPayload<RawTripTestPayload> {

  public static final String JSON_DATA_SCHEMA_STR = "{\"type\":\"record\",\"name\":\"triprec\",\"fields\":[{\"name\":\"number\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"}]}";
  public static final Schema JSON_DATA_SCHEMA = new Schema.Parser().parse(JSON_DATA_SCHEMA_STR);
  private static final MercifulJsonConverter JSON_CONVERTER = new MercifulJsonConverter();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private String partitionPath;
  private String rowKey;
  private byte[] jsonDataCompressed;
  private int dataSize;
  private boolean isDeleted;
  private Comparable orderingVal;

  public RawTripTestPayload(Option<String> jsonData, String rowKey, String partitionPath, String schemaStr,
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

  public RawTripTestPayload(String jsonData, String rowKey, String partitionPath, String schemaStr) throws IOException {
    this(Option.of(jsonData), rowKey, partitionPath, schemaStr, false, 0L);
  }

  public RawTripTestPayload(String jsonData) throws IOException {
    this.jsonDataCompressed = compressData(jsonData);
    this.dataSize = jsonData.length();
    Map<String, Object> jsonRecordMap = OBJECT_MAPPER.readValue(jsonData, Map.class);
    this.rowKey = jsonRecordMap.get("_row_key").toString();
    this.partitionPath = extractPartitionFromTimeField(jsonRecordMap.get("time").toString());
    this.isDeleted = false;
    this.orderingVal = Integer.valueOf(jsonRecordMap.getOrDefault("number", 0L).toString());
  }

  public RawTripTestPayload(GenericRecord record, Comparable orderingVal) {
    this.orderingVal = orderingVal;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
      DatumWriter w = new SpecificDatumWriter<>(record.getSchema());
      w.write(record, jsonEncoder);
      jsonEncoder.flush();
      out.flush();
      String jsonData = out.toString("UTF-8");
      Map<String, Object> jsonRecordMap = OBJECT_MAPPER.readValue(jsonData, Map.class);
      for (Schema.Field f : record.getSchema().getFields()) {
        Object fieldValue = jsonRecordMap.get(f.name());
        if (fieldValue instanceof Map) {
          Object unionValue = ((Map<?, ?>) fieldValue).values().iterator().next();
          jsonRecordMap.put(f.name(), unionValue);
        }
      }
      jsonData = OBJECT_MAPPER.writeValueAsString(jsonRecordMap);
      this.jsonDataCompressed = compressData(jsonData);
      this.dataSize = jsonData.length();
      this.rowKey = jsonRecordMap.get("_row_key").toString();
      this.partitionPath = extractPartitionFromTimeField(jsonRecordMap.get("time").toString());
      this.isDeleted = false;
    } catch (IOException e) {
      throw new IllegalStateException("Fail to instantiate.", e);
    }
  }

  /**
   * @deprecated PLEASE READ THIS CAREFULLY
   * <p>
   * Converting properly typed schemas into JSON leads to inevitable information loss, since JSON
   * encodes only representation of the record (with no schema accompanying it), therefore occasionally
   * losing nuances of the original data-types provided by the schema (for ex, with 1.23 literal it's
   * impossible to tell whether original type was Double or Decimal).
   * <p>
   * Multiplied by the fact that Spark 2 JSON schema inference has substantial gaps in it (see below),
   * it's **NOT RECOMMENDED** to use this method. Instead please consider using {@link AvroConversionUtils#createDataframe()}
   * method accepting list of {@link HoodieRecord} (as produced by the {@link HoodieTestDataGenerator}
   * to create Spark's {@code Dataframe}s directly.
   * <p>
   * REFs
   * https://medium.com/swlh/notes-about-json-schema-handling-in-spark-sql-be1e7f13839d
   */
  @Deprecated
  public static List<String> recordsToStrings(List<HoodieRecord> records) {
    return records.stream().map(RawTripTestPayload::recordToString).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
  }

  public static Option<String> recordToString(HoodieRecord record) {
    try {
      String str = ((RawTripTestPayload) record.getData()).getJsonData();
      str = "{" + str.substring(str.indexOf("\"timestamp\":"));
      // Remove the last } bracket
      str = str.substring(0, str.length() - 1);
      return Option.of(str + ", \"partition\": \"" + record.getPartitionPath() + "\"}");
    } catch (IOException e) {
      return Option.empty();
    }
  }

  public static List<String> deleteRecordsToStrings(List<HoodieKey> records) {
    return records.stream().map(record -> "{\"_row_key\": \"" + record.getRecordKey() + "\",\"partition\": \"" + record.getPartitionPath() + "\"}")
        .collect(Collectors.toList());
  }

  public static List<HoodieRecord> asDefaultPayloadRecords(List<HoodieRecord> records) throws IOException {
    return asDefaultPayloadRecords(records, false);
  }

  public static List<HoodieRecord> asDefaultPayloadRecords(List<HoodieRecord> records, boolean isDeleted) throws IOException {
    List<HoodieRecord> convertedRecords = new ArrayList<>();
    for (HoodieRecord r : records) {
      GenericRecord avroData = (GenericRecord) ((RawTripTestPayload) r.getData()).getRecordToInsert(AVRO_SCHEMA);
      avroData.put("_hoodie_is_deleted", isDeleted);
      convertedRecords.add(
          createHoodieRecordFromAvro(avroData, DefaultHoodieRecordPayload.class.getName(),
              "timestamp", Option.of(Pair.of("_row_key", "partition_path")),
              false, Option.empty(), false, Option.of(AVRO_SCHEMA)));
    }
    return convertedRecords;
  }

  public String getPartitionPath() {
    return partitionPath;
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

  public String getRowKey() {
    return rowKey;
  }

  public String getJsonData() throws IOException {
    return unCompressData(jsonDataCompressed);
  }

  public Map<String, Object> getJsonDataAsMap() throws IOException {
    if (jsonDataCompressed == null) {
      return Collections.emptyMap();
    }
    return OBJECT_MAPPER.readValue(getJsonData(), Map.class);
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

  public RawTripTestPayload clone() {
    try {
      return new RawTripTestPayload(unCompressData(jsonDataCompressed), rowKey, partitionPath, null);
    } catch (IOException e) {
      return null;
    }
  }

  public HoodieRecord toHoodieRecord() {
    return new HoodieAvroRecord(new HoodieKey(getRowKey(), getPartitionPath()), this);
  }

  public static String extractPartitionFromTimeField(String timeField) {
    return timeField.split("T")[0].replace("-", "/");
  }
}
