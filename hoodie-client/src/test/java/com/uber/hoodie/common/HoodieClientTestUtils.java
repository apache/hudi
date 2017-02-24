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

package com.uber.hoodie.common;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Utility methods to aid testing inside the HoodieClient module.
 */
public class HoodieClientTestUtils {


    public static List<WriteStatus> collectStatuses(Iterator<List<WriteStatus>> statusListItr) {
        List<WriteStatus> statuses = new ArrayList<>();
        while (statusListItr.hasNext()) {
            statuses.addAll(statusListItr.next());
        }
        return statuses;
    }

    public static Set<String> getRecordKeys(List<HoodieRecord> hoodieRecords) {
        Set<String> keys = new HashSet<>();
        for (HoodieRecord rec: hoodieRecords) {
            keys.add(rec.getRecordKey());
        }
        return keys;
    }

    private static void fakeMetaFile(String basePath, String commitTime, String suffix) throws IOException {
        String parentPath = basePath + "/"+ HoodieTableMetaClient.METAFOLDER_NAME;
        new File(parentPath).mkdirs();
        new File(parentPath + "/" + commitTime + suffix).createNewFile();
    }


    public static void fakeCommitFile(String basePath, String commitTime) throws IOException {
        fakeMetaFile(basePath, commitTime, HoodieTimeline.COMMIT_EXTENSION);
    }

    public static void fakeInFlightFile(String basePath, String commitTime) throws IOException {
        fakeMetaFile(basePath, commitTime, HoodieTimeline.INFLIGHT_EXTENSION);
    }

    public static void fakeDataFile(String basePath, String partitionPath, String commitTime, String fileId) throws Exception {
        fakeDataFile(basePath, partitionPath, commitTime, fileId, 0);
    }

    public static void fakeDataFile(String basePath, String partitionPath, String commitTime, String fileId, long length) throws Exception {
        String parentPath = String.format("%s/%s", basePath, partitionPath);
        new File(parentPath).mkdirs();
        String path = String.format("%s/%s", parentPath, FSUtils.makeDataFileName(commitTime, 0, fileId));
        new File(path).createNewFile();
        new RandomAccessFile(path, "rw").setLength(length);
    }
}
