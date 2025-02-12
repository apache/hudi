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

package org.apache.hudi.common.serialization;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDefaultSerializer {

  @Test
  void roundTripValidation() throws IOException {
    String partition = "1";
    String fileId1 = UUID.randomUUID().toString();
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    FileSlice fileSlice = new FileSlice(new HoodieFileGroupId(partition, fileId1), instant1.requestedTime(),
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName(instant1.requestedTime(), "1-0-1", fileId1, "parquet")), Collections.emptyList());

    DefaultSerializer<FileSlice> serializer = new DefaultSerializer<>();
    byte[] serializedValue = serializer.serialize(fileSlice);
    FileSlice output = serializer.deserialize(serializedValue);
    // validate round trip serialization working
    assertEquals(fileSlice, output);
  }
}
