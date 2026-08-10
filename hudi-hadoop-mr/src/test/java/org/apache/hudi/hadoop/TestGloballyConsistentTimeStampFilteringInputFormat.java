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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;

import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGloballyConsistentTimeStampFilteringInputFormat
    extends TestHoodieParquetInputFormat {

  @BeforeEach
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testInputFormatLoad() throws IOException {
    super.testInputFormatLoad();

    // set filtering timestamp to 0 now the timeline wont have any commits.
    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "0");

    Assertions.assertThrows(HoodieIOException.class, () -> inputFormat.getSplits(jobConf, 10));
    Assertions.assertThrows(HoodieIOException.class, () -> inputFormat.listStatus(jobConf));
  }

  @Test
  public void testInputFormatUpdates() throws IOException {
    super.testInputFormatUpdates();

    // set the globally replicated timestamp to 199 so only 100 is read and update is ignored.
    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "100");

    FileStatus[] files = inputFormat.listStatus(jobConf);
    assertEquals(10, files.length);

    ensureFilesInCommit("5 files have been updated to commit 200. but should get filtered out ",
        files,"200", 0);
    ensureFilesInCommit("We should see 10 files from commit 100 ", files, "100", 10);
  }

  @Override
  public void testIncrementalSimple() throws IOException {
    // setting filtering timestamp to zero should not in any way alter the result of the test which
    // pulls in zero files due to incremental ts being the actual commit time
    jobConf.set(HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP, "0");
    super.testIncrementalSimple();
  }

  @Override
  public void testIncrementalWithMultipleCommits() throws IOException {
    super.testIncrementalWithMultipleCommits();

    // set globally replicated timestamp to 400 so commits from 500, 600 does not show up
    InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, "400");
    InputFormatTestUtil.setupIncremental(jobConf, "100", HoodieHiveUtils.MAX_COMMIT_ALL);

    FileStatus[] files = inputFormat.listStatus(jobConf);

    assertEquals(
         5, files.length,"Pulling ALL commits from 100, should get us the 3 files from 400 commit, 1 file from 300 "
            + "commit and 1 file from 200 commit");
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 3 files from 400 commit",
        files, "400", 3);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 300 commit",
        files, "300", 1);
    ensureFilesInCommit("Pulling 3 commits from 100, should get us the 1 files from 200 commit",
        files, "200", 1);

    List<String> commits = Arrays.asList("100", "200", "300", "400", "500", "600");
    for (int idx = 0; idx < commits.size(); ++idx) {
      for (int jdx = 0; jdx < commits.size(); ++jdx) {
        InputFormatTestUtil.setupIncremental(jobConf, commits.get(idx), HoodieHiveUtils.MAX_COMMIT_ALL);
        InputFormatTestUtil.setupSnapshotMaxCommitTimeQueryMode(jobConf, commits.get(jdx));

        files = inputFormat.listStatus(jobConf);

        if (jdx <= idx) {
          assertEquals(0, files.length,"all commits should be filtered");
        } else {
          // only commits upto the timestamp is allowed
          for (FileStatus file : files) {
            String commitTs = FSUtils.getCommitTime(file.getPath().getName());
            assertTrue(commits.indexOf(commitTs) <= jdx);
            assertTrue(commits.indexOf(commitTs) > idx);
          }
        }
      }
    }
  }
}
