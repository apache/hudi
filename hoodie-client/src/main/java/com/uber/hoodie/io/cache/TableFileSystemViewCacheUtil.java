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


import com.google.common.base.Joiner;

public class TableFileSystemViewCacheUtil {

    public static final String FILE_NAME_SEPARATOR = "_";

    public static String generateCacheKey(String path, String fileId) {
        return Joiner.on(FILE_NAME_SEPARATOR).join(path, fileId);
    }

    public static String generateCacheValue(String taskPartitionId, String commitTime) {
        return Joiner.on(FILE_NAME_SEPARATOR).join(taskPartitionId, commitTime);
    }

    public static String generateCacheFileName(String fileId, String fileIdSuffix) {
        return Joiner.on(FILE_NAME_SEPARATOR).join(fileId, fileIdSuffix + ".parquet");
    }
}
