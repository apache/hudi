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

package com.uber.hoodie.common.util.collection;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.common.util.SpillableMapTestUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestRocksDbBasedMap {

  private static final String BASE_OUTPUT_PATH = "/tmp/";

  @Test
  public void testSimple() throws IOException, URISyntaxException {
    RocksDBBasedMap records = new RocksDBBasedMap(BASE_OUTPUT_PATH);
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    ((GenericRecord) iRecords.get(0)).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);

    // make sure records have spilled to disk
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    List<HoodieRecord> oRecords = new ArrayList<>();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      oRecords.add(rec);
      assert recordKeys.contains(rec.getRecordKey());
    }
    Assert.assertEquals(recordKeys.size(), oRecords.size());
  }
}
