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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.util.Map;

/**
 * Utilities used throughout the data source.
 * TODO: This file is partially copied from org.apache.hudi.DataSourceUtils.
 * Should be removed if Spark 2.x support is dropped.
 */
public class DataSourceUtilsForSpark2 {

  public static HoodieWriteConfig createHoodieConfig(String schemaStr, String basePath,
      String tblName, Map<String, String> parameters) {
    boolean asyncCompact = Boolean.parseBoolean(parameters.get(DataSourceWriteOptionsForSpark2.ASYNC_COMPACT_ENABLE_OPT_KEY()));
    boolean inlineCompact = !asyncCompact && parameters.get(DataSourceWriteOptionsForSpark2.TABLE_TYPE_OPT_KEY())
        .equals(DataSourceWriteOptionsForSpark2.MOR_TABLE_TYPE_OPT_VAL());
    // insert/bulk-insert combining to be true, if filtering for duplicates
    boolean combineInserts = Boolean.parseBoolean(parameters.get(DataSourceWriteOptionsForSpark2.INSERT_DROP_DUPS_OPT_KEY()));
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(basePath).withAutoCommit(false).combineInput(combineInserts, true);
    if (schemaStr != null) {
      builder = builder.withSchema(schemaStr);
    }

    return builder.forTable(tblName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(parameters.get(DataSourceWriteOptionsForSpark2.PAYLOAD_CLASS_OPT_KEY()))
            .withInlineCompaction(inlineCompact).build())
        // override above with Hoodie configs specified as options.
        .withProps(parameters).build();
  }

  public static String getCommitActionType(WriteOperationType operation, HoodieTableType tableType) {
    if (operation == WriteOperationType.INSERT_OVERWRITE) {
      return HoodieTimeline.REPLACE_COMMIT_ACTION;
    } else {
      return CommitUtils.getCommitActionType(tableType);
    }
  }
}
