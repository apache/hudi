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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests spillable map test utils.
 */
public class SpillableMapTestUtils {

  public static final String DUMMY_COMMIT_TIME = "DUMMY_COMMIT_TIME";
  public static final String DUMMY_FILE_ID = "DUMMY_FILE_ID";

  public static List<String> upsertRecords(List<IndexedRecord> iRecords,
      Map<String, HoodieRecord<? extends HoodieRecordPayload>> records) {
    List<String> recordKeys = new ArrayList<>();
    iRecords.forEach(r -> {
      String key = ((GenericRecord) r).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String partitionPath = ((GenericRecord) r).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
      recordKeys.add(key);
      HoodieRecord record =
          new HoodieAvroRecord<>(new HoodieKey(key, partitionPath), new HoodieAvroPayload(Option.of((GenericRecord) r)));
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("DUMMY_COMMIT_TIME", "DUMMY_FILE_ID"));
      record.seal();
      records.put(key, record);
    });
    return recordKeys;
  }
}
