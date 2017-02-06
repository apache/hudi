/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io.compact;

import java.util.List;

/**
 * Implementations of CompactionFilter allows prioritizing and filtering certain type of
 * compactions over other compactions.
 *
 * e.g. Filter in-efficient compaction like compacting a very large old parquet file with a small avro file
 */
public interface CompactionFilter {
    List<CompactionOperation> filter(List<CompactionOperation> input);

    // Default implementation - do not filter anything
    static CompactionFilter allowAll() {
        return s -> s;
    }
}
