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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.exception.HoodieException;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.createBaseFile;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;

public class ClusteringTestUtils {

  public static HoodieClusteringPlan createClusteringPlan(HoodieTableMetaClient metaClient, String instantTime, String fileId) {
    try {
      String basePath = metaClient.getBasePath();
      String partition = DEFAULT_PARTITION_PATHS[0];
      createBaseFile(basePath, partition, instantTime, fileId, 1);
      FileSlice slice = new FileSlice(partition, instantTime, fileId);
      slice.setBaseFile(new CompactionTestUtils.DummyHoodieBaseFile(Paths.get(basePath, partition,
              baseFileName(instantTime, fileId)).toString()));
      List<FileSlice>[] fileSliceGroups = new List[] {Collections.singletonList(slice)};
      HoodieClusteringPlan clusteringPlan = ClusteringUtils.createClusteringPlan("strategy", new HashMap<>(),
              fileSliceGroups, Collections.emptyMap());
      return clusteringPlan;
    } catch (Exception e) {
      throw new HoodieException(e.getMessage(), e);
    }
  }
}
