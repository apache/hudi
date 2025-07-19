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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieWriteStat;

import java.util.Collections;
import java.util.List;

/**
 * Class to hold list of {@link HoodieWriteStat} for data table and metadata table.
 */
public class TableWriteStats {

  private final List<HoodieWriteStat> dataTableWriteStats;
  private final List<HoodieWriteStat> metadataTableWriteStats;

  public TableWriteStats(List<HoodieWriteStat> dataTableWriteStats) {
    this(dataTableWriteStats, Collections.emptyList());
  }

  public TableWriteStats(List<HoodieWriteStat> dataTableWriteStats, List<HoodieWriteStat> metadataTableWriteStats) {
    this.dataTableWriteStats = dataTableWriteStats;
    this.metadataTableWriteStats = metadataTableWriteStats;
  }

  public List<HoodieWriteStat> getDataTableWriteStats() {
    return dataTableWriteStats;
  }

  public List<HoodieWriteStat> getMetadataTableWriteStats() {
    return metadataTableWriteStats;
  }

  public boolean isEmptyDataTableWriteStats() {
    return dataTableWriteStats.isEmpty();
  }
}
