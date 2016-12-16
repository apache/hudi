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


import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 *
 */
public class TestHoodieCommits {

    @Test
    public void testHoodieCommits() throws Exception {
        HoodieCommits commits = new HoodieCommits(Arrays.asList("001", "005", "004", "002"));
        assertFalse(commits.contains("003"));
        assertTrue(commits.contains("002"));
        assertEquals(Arrays.asList("004", "005"), commits.findCommitsAfter("003", 2));
        assertEquals(Arrays.asList("001", "002", "004"), commits.findCommitsInRange("000", "004"));
        assertEquals(commits.lastCommit(), commits.lastCommit(0));
        assertEquals("001", commits.lastCommit(3));
        assertEquals(null, commits.lastCommit(4));

        assertEquals(commits.max("001", "000"), "001");
        assertFalse(HoodieCommits.isCommit1After("001", "002"));
        assertFalse(HoodieCommits.isCommit1After("001", "001"));
        assertTrue(HoodieCommits.isCommit1After("003", "002"));
        assertTrue(HoodieCommits.isCommit1BeforeOrOn("003", "003"));
    }
}
