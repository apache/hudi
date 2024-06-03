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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * KeepByTimeStrategy will return expired partitions by their create time.
 */
public class KeepByCreationTimeStrategy extends KeepByTimeStrategy {

  public KeepByCreationTimeStrategy(HoodieTable hoodieTable, String instantTime) {
    super(hoodieTable, instantTime);
  }

  @Override
  protected List<String> getExpiredPartitionsForTimeStrategy(List<String> partitionPathsForTTL) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return partitionPathsForTTL.stream().parallel().filter(part -> {
      HoodiePartitionMetadata hoodiePartitionMetadata =
          new HoodiePartitionMetadata(metaClient.getStorage(), FSUtils.constructAbsolutePath(metaClient.getBasePath(), part));
      Option<String> instantOption = hoodiePartitionMetadata.readPartitionCreatedCommitTime();
      if (instantOption.isPresent()) {
        String instantTime = instantOption.get();
        return isPartitionExpired(instantTime);
      }
      return false;
    }).collect(Collectors.toList());
  }

}
