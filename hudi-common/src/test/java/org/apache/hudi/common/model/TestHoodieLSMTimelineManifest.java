/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link HoodieLSMTimelineManifest}.
 */
public class TestHoodieLSMTimelineManifest {
  @Test
  void testSerializeDeserialize() throws Exception {
    HoodieLSMTimelineManifest.LSMFileEntry entry1 = HoodieLSMTimelineManifest.LSMFileEntry.getInstance("file1", 1024);
    HoodieLSMTimelineManifest.LSMFileEntry entry2 = HoodieLSMTimelineManifest.LSMFileEntry.getInstance("file2", 2048);
    HoodieLSMTimelineManifest manifest = new HoodieLSMTimelineManifest(Arrays.asList(entry1, entry2));
    String expected = "{\n"
        + "  \"files\" : [ {\n"
        + "    \"fileName\" : \"file1\",\n"
        + "    \"fileLen\" : 1024\n"
        + "  }, {\n"
        + "    \"fileName\" : \"file2\",\n"
        + "    \"fileLen\" : 2048\n"
        + "  } ]\n"
        + "}";
    // serialization
    assertThat(manifest.toJsonString(), is(expected));
    // deserialization
    HoodieLSMTimelineManifest deserialized = HoodieLSMTimelineManifest.fromJsonString(expected, HoodieLSMTimelineManifest.class);
    assertThat(deserialized.getFiles().size(), is(2));
    assertThat(deserialized.getFiles().stream().map(HoodieLSMTimelineManifest.LSMFileEntry::getFileName).collect(Collectors.joining(",")), is("file1,file2"));
    assertThat(deserialized.getFiles().stream().map(entry -> String.valueOf(entry.getFileLen())).collect(Collectors.joining(",")), is("1024,2048"));
  }
}
