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

package com.uber.hoodie.common.table.string;

import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

public class MockHoodieTimeline extends HoodieDefaultTimeline {
    private String fileExt;

    public MockHoodieTimeline(FileSystem fs, String metaPath, String fileExtension)
        throws IOException {
        super(fs, metaPath, fileExtension);
        this.fileExt = fileExtension;
    }

    public MockHoodieTimeline(Stream<String> instants, Stream<String> inflights)
        throws IOException {
        super(instants, inflights);
    }

    @Override
    public HoodieTimeline reload() throws IOException {
        return new MockHoodieTimeline(fs, metaPath, fileExt);
    }

    @Override
    public Optional<byte[]> readInstantDetails(String instant) {
        return Optional.empty();
    }

    @Override
    protected String getInflightFileName(String instant) {
        return HoodieTestUtils.makeInflightTestFileName(instant);
    }

    @Override
    protected String getCompletedFileName(String instant) {
        return HoodieTestUtils.makeTestFileName(instant);
    }

    @Override
    protected String getTimelineName() {
        return "mock-test";
    }
}
