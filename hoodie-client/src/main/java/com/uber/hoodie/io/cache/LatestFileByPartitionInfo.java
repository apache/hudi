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

import java.io.Serializable;
import java.util.Map;

/**
 * LatestFileByPartitionInfo is the storage abstraction for latest files
 */
public class LatestFileByPartitionInfo implements Serializable {
    private Map<String, String> latestFileByPartition;

    public LatestFileByPartitionInfo(Map<String, String> latestFileByPartition) {
        this.latestFileByPartition = latestFileByPartition;
    }

    public Map<String, String> getLatestFileByPartition() {
        return this.latestFileByPartition;
    }
}
