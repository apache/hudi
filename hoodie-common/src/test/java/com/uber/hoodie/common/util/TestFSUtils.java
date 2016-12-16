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

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class TestFSUtils {

    @Test
    public void testMakeDataFileName() {
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        int taskPartitionId = 2;
        String fileName = UUID.randomUUID().toString();
        assertTrue(FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName)
                .equals(fileName + "_" + taskPartitionId + "_" + commitTime + ".parquet"));
    }

    @Test
    public void testMaskFileName() {
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        int taskPartitionId = 2;
        assertTrue(FSUtils.maskWithoutFileId(commitTime, taskPartitionId)
                .equals("*_" + taskPartitionId + "_" + commitTime + ".parquet"));
    }

    @Test
    public void testGetCommitTime() {
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        int taskPartitionId = 2;
        String fileName = UUID.randomUUID().toString();
        String fullFileName = FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName);
        assertTrue(FSUtils.getCommitTime(fullFileName).equals(commitTime));
    }

    @Test
    public void testGetCommitFromCommitFile() {
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String commitFileName = FSUtils.makeCommitFileName(commitTime);
        assertTrue(FSUtils.getCommitFromCommitFile(commitFileName).equals(commitTime));
    }

    @Test
    public void testGetFileNameWithoutMeta() {
        String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        int taskPartitionId = 2;
        String fileName = UUID.randomUUID().toString();
        String fullFileName = FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName);
        assertTrue(FSUtils.getFileId(fullFileName).equals(fileName));
    }
}
