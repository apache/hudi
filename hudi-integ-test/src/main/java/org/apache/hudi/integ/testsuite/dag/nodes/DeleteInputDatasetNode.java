/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Deletes all input except latest batch. Mostly used in insert_overwrite operations.
 */
@Slf4j
public class DeleteInputDatasetNode extends DagNode<Boolean> {

  public DeleteInputDatasetNode(DeltaConfig.Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {

    String latestBatch = String.valueOf(context.getWriterContext().getDeltaGenerator().getBatchId());

    if (config.isDeleteInputDataExceptLatest()) {
      String inputPathStr = context.getHoodieTestSuiteWriter().getCfg().inputBasePath;
      FileSystem fs = new Path(inputPathStr)
          .getFileSystem(context.getHoodieTestSuiteWriter().getConfiguration());
      FileStatus[] fileStatuses = fs.listStatus(new Path(inputPathStr));
      for (FileStatus fileStatus : fileStatuses) {
        if (!fileStatus.getPath().getName().equals(latestBatch)) {
          log.debug("Micro batch to be deleted {}", fileStatus.getPath());
          fs.delete(fileStatus.getPath(), true);
        }
      }
    }
  }
}
