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

package com.uber.hoodie.common.table.timeline;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;

public class HoodieCleanerTimeline extends HoodieDefaultTimeline {
    public HoodieCleanerTimeline(FileSystem fs, String path) {
        super(fs, path, HoodieTableMetaClient.CLEAN_EXTENSION);
    }

    @Override
    public HoodieTimeline reload() throws IOException {
        return new HoodieCleanerTimeline(fs, metaPath);
    }

    @Override
    public Optional<byte[]> readInstantDetails(String instant) {
        // TODO - Nothing about the clean written today - this should change
        return Optional.empty();
    }

    @Override
    protected String getInflightFileName(String instant) {
        return HoodieTableMetaClient.makeInflightCleanerFileName(instant);
    }

    @Override
    protected String getCompletedFileName(String instant) {
        return HoodieTableMetaClient.makeCleanerFileName(instant);
    }

    @Override
    protected String getTimelineName() {
        return "cleaner";
    }
}
