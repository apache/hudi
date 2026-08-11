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

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Record-index lookup that refuses to claim time travel beyond the MDT's synced instant. */
public final class MetadataRecordIndexLookup implements RecordIndexLookup {

  private static final long serialVersionUID = 1L;

  private final HoodieTableMetadata metadataTable;

  public MetadataRecordIndexLookup(HoodieTableMetadata metadataTable) {
    this.metadataTable = Objects.requireNonNull(metadataTable, "metadataTable");
  }

  @Override
  public Map<String, HoodieRecordGlobalLocation> lookup(
      List<String> recordKeys, String tableInstant) {
    Option<String> syncedInstant = metadataTable.getSyncedInstantTime();
    if (!syncedInstant.isPresent() || !syncedInstant.get().equals(tableInstant)) {
      throw new IllegalStateException(
          "Record index is not pinned to requested table instant " + tableInstant
              + "; syncedInstant=" + (syncedInstant.isPresent() ? syncedInstant.get() : "absent"));
    }
    Map<String, HoodieRecordGlobalLocation> locations = new HashMap<>();
    metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys))
        .collectAsList()
        .forEach(pair -> locations.put(pair.getLeft(), pair.getRight()));
    return locations;
  }
}
