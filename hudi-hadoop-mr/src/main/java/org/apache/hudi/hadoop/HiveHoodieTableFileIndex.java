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

package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.BaseHoodieTableFileIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of {@link BaseHoodieTableFileIndex} for Hive-based query engines
 */
public class HiveHoodieTableFileIndex extends BaseHoodieTableFileIndex {

  public static final Logger LOG = LoggerFactory.getLogger(HiveHoodieTableFileIndex.class);

  public HiveHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  HoodieTableQueryType queryType,
                                  List<Path> queryPaths,
                                  Option<String> specifiedQueryInstant,
                                  boolean shouldIncludePendingCommits
  ) {
    super(engineContext,
        metaClient,
        configProperties,
        queryType,
        queryPaths,
        specifiedQueryInstant,
        shouldIncludePendingCommits,
        true,
        new NoopCache());
  }

  @Override
  public Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
    // NOTE: Parsing partition path into partition column values isn't required on Hive,
    //       since Hive does partition pruning in a different way (based on the input-path being
    //       fetched by the query engine)
    return new Object[0];
  }

  static class NoopCache implements FileStatusCache {
    @Override
    public Option<FileStatus[]> get(Path path) {
      return Option.empty();
    }

    @Override
    public void put(Path path, FileStatus[] leafFiles) {
      // no-op
    }

    @Override
    public void invalidate() {
      // no-op
    }
  }
}
