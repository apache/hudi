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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.commit.SmallFile;

import java.util.Collections;
import java.util.List;

/**
 * BucketAssigner for INSERT OVERWRITE and INSERT OVERWRITE TABLE operations,
 * this assigner always skip the existing small files because of the 'OVERWRITE' semantics.
 *
 * <p>Note: assumes the index can always index log files for Flink write.
 */
public class OverwriteBucketAssigner extends BucketAssigner {
  public OverwriteBucketAssigner(
      int taskID,
      int numTasks,
      HoodieFlinkEngineContext context,
      HoodieWriteConfig config) {
    super(taskID, numTasks, context, config);
  }

  @Override
  protected List<SmallFile> getSmallFiles(String partitionPath) {
    return Collections.emptyList();
  }
}
