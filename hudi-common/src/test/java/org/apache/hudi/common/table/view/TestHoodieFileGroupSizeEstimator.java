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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.ObjectSizeCalculator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieFileGroupSizeEstimator {
  @Test
  void estimatorSkipsTimeline() {
    HoodieFileGroup fileGroup1 = mock(HoodieFileGroup.class);
    HoodieFileGroup fileGroup2 = mock(HoodieFileGroup.class);

    // setup mocks
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId("path1", UUID.randomUUID().toString());
    List<FileSlice> fileSlices = Collections.singletonList(new FileSlice(fileGroupId, "001",
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName("001", "1-0-1", fileGroupId.getFileId(), "parquet")), Collections.emptyList()));
    when(fileGroup1.getFileGroupId()).thenReturn(fileGroupId);
    when(fileGroup1.getAllFileSlices()).thenReturn(fileSlices.stream()).thenReturn(fileSlices.stream());

    when(fileGroup2.getFileGroupId()).thenReturn(new HoodieFileGroupId("path2", UUID.randomUUID().toString()));
    when(fileGroup2.getAllFileSlices()).thenReturn(Stream.empty()).thenReturn(Stream.empty());

    long result = new HoodieFileGroupSizeEstimator().sizeEstimate(Arrays.asList(fileGroup1, fileGroup2));
    Assertions.assertTrue(result > 0);
    verify(fileGroup1, never()).getTimeline();
    verify(fileGroup2, never()).getTimeline();
  }

  @Test
  void estimatorWithManyFileSlices() {
    HoodieFileGroup fileGroup1 = mock(HoodieFileGroup.class);
    HoodieFileGroup fileGroup2 = mock(HoodieFileGroup.class);

    // setup mocks
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId("path1", UUID.randomUUID().toString());
    List<FileSlice> fileSlices = IntStream.range(1, 100).mapToObj(i -> new FileSlice(fileGroupId, "001",
        new HoodieBaseFile("/tmp/" + FSUtils.makeBaseFileName("00" + i, "1-0-1", fileGroupId.getFileId(), "parquet")), Collections.emptyList()))
        .collect(Collectors.toList());
    when(fileGroup1.getFileGroupId()).thenReturn(fileGroupId);
    when(fileGroup1.getAllFileSlices()).thenReturn(fileSlices.stream()).thenReturn(fileSlices.stream());

    when(fileGroup2.getFileGroupId()).thenReturn(new HoodieFileGroupId("path2", UUID.randomUUID().toString()));
    when(fileGroup2.getAllFileSlices()).thenReturn(Stream.empty()).thenReturn(Stream.empty());

    long result = new HoodieFileGroupSizeEstimator().sizeEstimate(Arrays.asList(fileGroup1, fileGroup2));
    long exactSize = ObjectSizeCalculator.getObjectSize(fileSlices) + ObjectSizeCalculator.getObjectSize(fileGroupId) * 2;
    Assertions.assertTrue(Math.abs(result - exactSize) / (1.0 * exactSize) < 0.1); // ensure that sampling is accurate within 10%
    verify(fileGroup1, never()).getTimeline();
    verify(fileGroup2, never()).getTimeline();
  }
}
