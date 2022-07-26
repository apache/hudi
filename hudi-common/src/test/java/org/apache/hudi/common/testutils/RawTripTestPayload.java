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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Example row change event based on some example data used by testcases. The data avro schema is
 * src/test/resources/schema1.
 */
public class RawTripTestPayload extends GenericTestPayload implements HoodieRecordPayload<RawTripTestPayload> {

  public RawTripTestPayload(Option<String> jsonData, String rowKey, String partitionPath, String schemaStr,
      Boolean isDeleted, Comparable orderingVal) throws IOException {
    super(jsonData, rowKey, partitionPath, schemaStr, isDeleted, orderingVal);
  }

  public RawTripTestPayload(String jsonData, String rowKey, String partitionPath, String schemaStr) throws IOException {
    this(Option.of(jsonData), rowKey, partitionPath, schemaStr, false, 0L);
  }

  public RawTripTestPayload(String jsonData) throws IOException {
    super(jsonData);
  }

  /**
   * @deprecated PLEASE READ THIS CAREFULLY
   *
   * Converting properly typed schemas into JSON leads to inevitable information loss, since JSON
   * encodes only representation of the record (with no schema accompanying it), therefore occasionally
   * losing nuances of the original data-types provided by the schema (for ex, with 1.23 literal it's
   * impossible to tell whether original type was Double or Decimal).
   *
   * Multiplied by the fact that Spark 2 JSON schema inference has substantial gaps in it (see below),
   * it's **NOT RECOMMENDED** to use this method. Instead please consider using {@link AvroConversionUtils#createDataframe()}
   * method accepting list of {@link HoodieRecord} (as produced by the {@link HoodieTestDataGenerator}
   * to create Spark's {@code Dataframe}s directly.
   *
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

  public RawTripTestPayload preCombine(RawTripTestPayload oldValue) {
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
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
}
