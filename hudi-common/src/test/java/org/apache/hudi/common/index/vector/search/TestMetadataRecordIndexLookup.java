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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestMetadataRecordIndexLookup {

  @Test
  void rejectsLookupWhenRliIsNotPinnedToQueryInstant() {
    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getSyncedInstantTime()).thenReturn(Option.of("002"));

    assertThrows(IllegalStateException.class,
        () -> new MetadataRecordIndexLookup(metadata).lookup(Collections.singletonList("id"), "001"));
  }

  @Test
  void returnsLiveLocationsAtPinnedInstant() {
    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getSyncedInstantTime()).thenReturn(Option.of("002"));
    HoodiePairData<String, HoodieRecordGlobalLocation> pairs = mock(HoodiePairData.class);
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p", "002", "f");
    when(pairs.collectAsList()).thenReturn(Arrays.asList(Pair.of("id", location)));
    when(metadata.readRecordIndexLocationsWithKeys(any())).thenReturn(pairs);

    Map<String, HoodieRecordGlobalLocation> result =
        new MetadataRecordIndexLookup(metadata).lookup(Collections.singletonList("id"), "002");

    assertEquals(location, result.get("id"));
  }
}
