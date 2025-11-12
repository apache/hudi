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
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.testutils.MockHoodieTimeline;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestHoodieFileGroupSerializer {

  @Test
  void serializerReusesTimelineInstance() throws IOException {
    // file groups 1 and 2 reference the same instance
    String partition = "1";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    FileSlice fileSlice1 = new FileSlice(new HoodieFileGroupId(partition, fileId1), instant1.requestedTime(),
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName(instant1.requestedTime(), "1-0-1", fileId1, "parquet")), Collections.emptyList());
    FileSlice fileSlice2 = new FileSlice(new HoodieFileGroupId(partition, fileId2), instant1.requestedTime(),
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName(instant1.requestedTime(), "1-0-1", fileId2, "parquet")), Collections.emptyList());
    HoodieTimeline mockTimeline1 = new MockHoodieTimeline(Collections.singletonList(instant1));

    HoodieFileGroup hoodieFileGroup1 = new HoodieFileGroup(partition, fileId1, mockTimeline1);
    hoodieFileGroup1.addFileSlice(fileSlice1);
    HoodieFileGroup hoodieFileGroup2 = new HoodieFileGroup(partition, fileId2, mockTimeline1);
    hoodieFileGroup2.addFileSlice(fileSlice2);

    // file group 3 references a new instance
    String fileId3 = UUID.randomUUID().toString();
    HoodieInstant instant2 = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "002", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieTimeline mockTimeline2 = new MockHoodieTimeline(Collections.singletonList(instant2));
    FileSlice fileSlice3 = new FileSlice(new HoodieFileGroupId(partition, fileId3), instant1.requestedTime(),
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName(instant2.requestedTime(), "1-0-1", fileId3, "parquet")), Collections.emptyList());
    HoodieFileGroup hoodieFileGroup3 = new HoodieFileGroup(partition, fileId3, mockTimeline2);
    hoodieFileGroup3.addFileSlice(fileSlice3);

    List<HoodieFileGroup> inputs = Arrays.asList(hoodieFileGroup1, hoodieFileGroup2, hoodieFileGroup3);
    HoodieFileGroupSerializer serializer = new HoodieFileGroupSerializer();
    byte[] serializedValue = serializer.serialize(inputs);
    List<HoodieFileGroup> outputs = serializer.deserialize(serializedValue);
    // validate round trip serialization working
    assertEquals(inputs, outputs);
    // validate that the timeline instances are the same object, not just equivalent
    assertSame(mockTimeline1, outputs.get(0).getTimeline());
    assertSame(mockTimeline1, outputs.get(1).getTimeline());
    assertSame(mockTimeline2, outputs.get(2).getTimeline());
  }
}
