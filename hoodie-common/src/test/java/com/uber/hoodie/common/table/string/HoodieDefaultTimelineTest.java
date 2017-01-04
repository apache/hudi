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
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class HoodieDefaultTimelineTest {
    private HoodieTimeline timeline;
    private HoodieTableMetaClient metaClient;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.metaClient = HoodieTestUtils.initOnTemp();
    }

    @After
    public void tearDown() throws Exception {
        HoodieTestUtils.fs.delete(new Path(this.metaClient.getBasePath()), true);
    }

    @Test
    public void testLoadingInstantsFromFiles() throws IOException {
        timeline =
            new MockHoodieTimeline(HoodieTestUtils.fs, metaClient.getMetaPath(), ".test");
        timeline.saveInstantAsComplete("1", Optional.empty());
        timeline.saveInstantAsComplete("3", Optional.empty());
        timeline.saveInstantAsComplete("5", Optional.empty());
        timeline.saveInstantAsComplete("8", Optional.empty());
        timeline.saveInstantAsInflight("9");
        timeline = timeline.reload();

        assertEquals("Total instants should be 4", 4, timeline.getTotalInstants());
        HoodieTestUtils
            .assertStreamEquals("Check the instants stream", Stream.of("1", "3", "5", "8"),
                timeline.getInstants());
        assertTrue("Inflights should be present in the timeline", timeline.hasInflightInstants());
        HoodieTestUtils.assertStreamEquals("Check the inflights stream", Stream.of("9"),
            timeline.getInflightInstants());
    }

    @Test
    public void testTimelineOperationsBasic() throws Exception {
        timeline = new MockHoodieTimeline(Stream.empty(), Stream.empty());
        assertFalse(timeline.hasInstants());
        assertFalse(timeline.hasInflightInstants());
        assertEquals("", 0, timeline.getTotalInstants());
        assertEquals("", Optional.empty(), timeline.firstInstant());
        assertEquals("", Optional.empty(), timeline.nthInstant(5));
        assertEquals("", Optional.empty(), timeline.nthInstant(-1));
        assertEquals("", Optional.empty(), timeline.lastInstant());
        assertFalse("", timeline.containsInstant("01"));
    }

    @Test
    public void testTimelineOperations() throws Exception {
        timeline = new MockHoodieTimeline(
            Stream.of("01", "03", "05", "07", "09", "11", "13", "15", "17", "19"),
            Stream.of("21", "23"));
        HoodieTestUtils.assertStreamEquals("", Stream.of("05", "07", "09", "11"),
            timeline.findInstantsInRange("04", "11"));
        HoodieTestUtils
            .assertStreamEquals("", Stream.of("09", "11"), timeline.findInstantsAfter("07", 2));
        assertTrue(timeline.hasInstants());
        assertTrue(timeline.hasInflightInstants());
        assertEquals("", 10, timeline.getTotalInstants());
        assertEquals("", "01", timeline.firstInstant().get());
        assertEquals("", "11", timeline.nthInstant(5).get());
        assertEquals("", "19", timeline.lastInstant().get());
        assertEquals("", "09", timeline.nthFromLastInstant(5).get());
        assertTrue("", timeline.containsInstant("09"));
        assertFalse("", timeline.isInstantBeforeTimelineStarts("02"));
        assertTrue("", timeline.isInstantBeforeTimelineStarts("00"));
    }
}
