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

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Node to validate data set sanity like total file versions retained, has cleaning happened, has archival happened, etc.
 */
public class ValidateAsyncOperations extends DagNode<Option<String>> {

  private static Logger log = LoggerFactory.getLogger(ValidateAsyncOperations.class);

  public ValidateAsyncOperations(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    if (config.getIterationCountToExecute() == curItrCount) {
      try {
        log.warn("Executing ValidateHoodieAsyncOperations node {} with target base path {} ", this.getName(),
            executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath);
        String basePath = executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath;

        int maxCommitsRetained = executionContext.getHoodieTestSuiteWriter().getWriteConfig().getCleanerCommitsRetained() + 1;
        FileSystem fs = FSUtils.getFs(basePath, executionContext.getHoodieTestSuiteWriter().getConfiguration());
        Map<String, Integer> fileIdCount = new HashMap<>();

        AtomicInteger maxVal = new AtomicInteger();
        List<String> partitionPaths = FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, basePath);
        for (String partitionPath : partitionPaths) {
          List<FileStatus> fileStatuses = Arrays.stream(FSUtils.getAllDataFilesInPartition(fs, new Path(basePath + "/" + partitionPath))).collect(Collectors.toList());
          fileStatuses.forEach(entry -> {
            String fileId = FSUtils.getFileId(entry.getPath().getName());
            fileIdCount.computeIfAbsent(fileId, k -> 0);
            fileIdCount.put(fileId, fileIdCount.get(fileId) + 1);
            maxVal.set(Math.max(maxVal.get(), fileIdCount.get(fileId)));
          });
        }
        if (maxVal.get() > maxCommitsRetained) {
          throw new AssertionError("Total commits (" + maxVal + ") retained exceeds max value of " + maxCommitsRetained + ", total commits : ");
        }

        if (config.validateArchival() || config.validateClean()) {
          final Pattern ARCHIVE_FILE_PATTERN =
              Pattern.compile("\\.commits_\\.archive\\..*");
          final Pattern CLEAN_FILE_PATTERN =
              Pattern.compile(".*\\.clean\\..*");

          String metadataPath = executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/.hoodie";
          FileStatus[] metaFileStatuses = fs.listStatus(new Path(metadataPath));
          boolean cleanFound = false;
          for (FileStatus fileStatus : metaFileStatuses) {
            Matcher cleanFileMatcher = CLEAN_FILE_PATTERN.matcher(fileStatus.getPath().getName());
            if (cleanFileMatcher.matches()) {
              cleanFound = true;
              break;
            }
          }

          String archivalPath = executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath + "/.hoodie/archived";
          metaFileStatuses = fs.listStatus(new Path(archivalPath));
          boolean archFound = false;
          for (FileStatus fileStatus : metaFileStatuses) {
            Matcher archFileMatcher = ARCHIVE_FILE_PATTERN.matcher(fileStatus.getPath().getName());
            if (archFileMatcher.matches()) {
              archFound = true;
            }
          }

          if (config.validateArchival() && !archFound) {
            throw new AssertionError("Archival NotFound in " + metadataPath);
          }

          if (config.validateClean() && !cleanFound) {
            throw new AssertionError("Clean commits NotFound in " + metadataPath);
          }
        }
      } catch (Exception e) {
        log.warn("Exception thrown in ValidateHoodieAsyncOperations Node :: " + e.getCause() + ", msg :: " + e.getMessage());
        throw e;
      }
    }
  }
}
