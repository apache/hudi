/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.common.util;

import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions related to accessing the file storage
 */
public class FSUtils {

    private static final Logger LOG = LogManager.getLogger(FSUtils.class);

    public static FileSystem getFs() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to get instance of " + FileSystem.class.getName(),
                e);
        }
        LOG.info(String.format("Hadoop Configuration: fs.defaultFS: [%s], Config:[%s], FileSystem: [%s]",
                conf.getRaw("fs.defaultFS"), conf.toString(), fs.toString()));
        return fs;
    }

    public static String makeDataFileName(String commitTime, int taskPartitionId, String fileId) {
        return String.format("%s_%d_%s.parquet", fileId, taskPartitionId, commitTime);
    }

    public static String maskWithoutFileId(String commitTime, int taskPartitionId) {
        return String.format("*_%s_%s.parquet", taskPartitionId, commitTime);
    }

    public static String maskWithoutTaskPartitionId(String commitTime, String fileId) {
        return String.format("%s_*_%s.parquet", fileId, commitTime);
    }

    public static String maskWithOnlyCommitTime(String commitTime) {
        return String.format("*_*_%s.parquet", commitTime);
    }

    public static String getCommitFromCommitFile(String commitFileName) {
        return commitFileName.split("\\.")[0];
    }

    public static String getCommitTime(String fullFileName) {
        return fullFileName.split("_")[2].split("\\.")[0];
    }

    public static long getFileSize(FileSystem fs, Path path) throws IOException {
        return fs.listStatus(path)[0].getLen();
    }

    public static String globAllFiles(String basePath) {
        return String.format("%s/*/*/*/*", basePath);
    }

    // TODO (weiy): rename the function for better readability
    public static String getFileId(String fullFileName) {
        return fullFileName.split("_")[0];
    }

    /**
     * Obtain all the partition paths, that are present in this table.
     */
    public static List<String> getAllPartitionPaths(FileSystem fs, String basePath) throws IOException {
        List<String> partitionsToClean = new ArrayList<>();
        // TODO(vc): For now, assume partitions are two levels down from base path.
        FileStatus[] folders = fs.globStatus(new Path(basePath + "/*/*/*"));
        for (FileStatus status : folders) {
            Path path = status.getPath();
            partitionsToClean.add(String.format("%s/%s/%s",
                    path.getParent().getParent().getName(),
                    path.getParent().getName(),
                    path.getName()));
        }
        return partitionsToClean;
    }

}
