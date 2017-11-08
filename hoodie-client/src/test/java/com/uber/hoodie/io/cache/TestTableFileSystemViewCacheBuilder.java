/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.cache;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestTableFileSystemViewCacheBuilder {

    @Test
    public void testBuildLatestCache() {
        List<Tuple2<String, HoodieDataFile>> partitionFilePairList = new ArrayList<>();
        int maxCommits = 10;
        int maxFiles = 100;
        String partition = "2017/01/01";

        for (int i = 0; i < maxFiles; ++i) {
            Random randomCommitTimeGen = new Random();
            boolean hasLatestCommit = false;
            String fileId = "test-data-file-" + i;

            for (int j = 0; j < maxCommits; ++j) {
                long randomCommitTime = randomCommitTimeGen.nextInt(maxCommits);
                if (j == maxCommits - 1 && !hasLatestCommit) {
                    randomCommitTime = maxCommits - 1;
                }
                if (randomCommitTime == maxCommits - 1) {
                    hasLatestCommit = true;
                }
                String commitTime = String.format("%08d", randomCommitTime);
                String dataFileName = FSUtils.makeDataFileName(commitTime, j, fileId);
                FileStatus fileStatus = new FileStatus();
                fileStatus.setPath(new Path(dataFileName));
                partitionFilePairList.add(new Tuple2<>(partition, new HoodieDataFile(fileStatus)));
            }
        }

        LatestFileByPartitionInfo info = new TableFileSystemViewCacheBuilder(partitionFilePairList).build();

        assertEquals("Latest files should be equal to unique file IDs", maxFiles, info.getLatestFileByPartition().size());
        for (int i = 0; i < maxFiles; ++i) {
            String fileId = "test-data-file-" + i;
            String latestFile = TableFileSystemViewCacheUtil.generateCacheFileName(fileId,
                info.getLatestFileByPartition().get(TableFileSystemViewCacheUtil.generateCacheKey(partition, fileId)));

            String commitTime = FSUtils.getCommitTime(latestFile);
            assertEquals("Latest commit time should be (maxCommits-1)",
                (maxCommits - 1), Integer.valueOf(commitTime).intValue());
        }
    }
}
