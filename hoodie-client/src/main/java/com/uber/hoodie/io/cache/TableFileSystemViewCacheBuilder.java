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

import com.google.common.collect.ImmutableMap;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.util.FSUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableFileSystemViewCacheBuilder {
    private List<Tuple2<String, String>> partitionFilePairList;

    public TableFileSystemViewCacheBuilder(List<Tuple2<String, HoodieDataFile>> partitionFilePair) {
        this.partitionFilePairList = partitionFilePair.stream()
            .map(tuple -> new Tuple2<>(tuple._1(), tuple._2().getFileName()))
            .collect(Collectors.toList());
    }

    public LatestFileByPartitionInfo build() {
        Map<String, String> latestFileByPartition = new HashMap<>();

        for (Tuple2<String, String> partitionFilePair : partitionFilePairList) {
            final String path = partitionFilePair._1();
            final String fileName = partitionFilePair._2();
            final String fileId = FSUtils.getFileId(fileName);
            final String pathFileIdKey = TableFileSystemViewCacheUtil.generateCacheKey(path, fileId);
            final String taskPartitionId = FSUtils.getTaskPartitionId(fileName);
            final String newCommitTime = FSUtils.getCommitTime(fileName);

            if (!latestFileByPartition.containsKey(pathFileIdKey)) {
                latestFileByPartition.put(pathFileIdKey, TableFileSystemViewCacheUtil.generateCacheValue(taskPartitionId, newCommitTime));
            } else {
                final String oldCommitTime = FSUtils.getCommitTime(TableFileSystemViewCacheUtil.generateCacheFileName(
                    fileId, latestFileByPartition.get(pathFileIdKey)));
                if (newCommitTime.compareTo(oldCommitTime) > 0) {
                    latestFileByPartition.put(pathFileIdKey,
                        TableFileSystemViewCacheUtil.generateCacheValue(taskPartitionId, newCommitTime));
                }
            }
        }

        return new LatestFileByPartitionInfo(ImmutableMap.copyOf(latestFileByPartition));
    }
}
