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

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.FSUtils;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class HoodieTestUtils {

    public static final String RAW_TRIPS_TEST_NAME = "raw_trips";

    public static final void initializeHoodieDirectory(String basePath) throws IOException {
        new File(basePath + "/" + HoodieTableMetadata.METAFOLDER_NAME).mkdirs();
        Properties properties = new Properties();
        properties.setProperty(HoodieTableMetadata.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
        properties.setProperty(HoodieTableMetadata.HOODIE_TABLE_TYPE_PROP_NAME, HoodieTableMetadata.DEFAULT_TABLE_TYPE.name());
        FileWriter fileWriter = new FileWriter(new File(basePath + "/.hoodie/hoodie.properties"));
        try {
            properties.store(fileWriter, "");
        } finally {
            fileWriter.close();
        }
    }

    public static final String initializeTempHoodieBasePath() throws IOException {
        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        String basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.initializeHoodieDirectory(basePath);
        return basePath;
    }

    public static final String getNewCommitTime() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }

    public static final void createCommitFiles(String basePath, String... commitTimes) throws IOException {
        for (String commitTime: commitTimes) {
            new File(basePath + "/" + HoodieTableMetadata.METAFOLDER_NAME+ "/" + FSUtils.makeCommitFileName(commitTime)).createNewFile();
        }
    }

    public static final void createInflightCommitFiles(String basePath, String... commitTimes) throws IOException {
        for (String commitTime: commitTimes) {
            new File(basePath + "/" + HoodieTableMetadata.METAFOLDER_NAME+ "/" + FSUtils.makeInflightCommitFileName(commitTime)).createNewFile();
        }
    }

    public static final String createNewDataFile(String basePath, String partitionPath, String commitTime) throws IOException {
        String fileID = UUID.randomUUID().toString();
        return createDataFile(basePath, partitionPath, commitTime, fileID);
    }

    public static final String createDataFile(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        String folderPath = basePath + "/" + partitionPath + "/";
        new File(folderPath).mkdirs();
        new File(folderPath + FSUtils.makeDataFileName(commitTime, 1, fileID)).createNewFile();
        return fileID;
    }

    public static final boolean doesDataFileExist(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        return new File(basePath + "/" + partitionPath + "/" + FSUtils.makeDataFileName(commitTime, 1, fileID)).exists();
    }

    public static final boolean doesCommitExist(String basePath, String commitTime) {
        return new File(basePath + "/" + HoodieTableMetadata.METAFOLDER_NAME+ "/" + commitTime + HoodieTableMetadata.COMMIT_FILE_SUFFIX).exists();
    }

    public static final boolean doesInflightExist(String basePath, String commitTime) {
        return new File(basePath + "/" + HoodieTableMetadata.METAFOLDER_NAME+ "/" + commitTime + HoodieTableMetadata.INFLIGHT_FILE_SUFFIX).exists();
    }
}
