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

package org.apache.hudi.common.util;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests file I/O utils.
 */
public class TestFileIOUtils extends HoodieCommonTestHarness {

  @Test
  public void testMkdirAndDelete() throws IOException {
    try {
      FileIOUtils.mkdir(tempDir.toFile());
    } catch (IOException e) {
      fail("Should not error out if dir exists already");
    }
    File dir = tempDir.resolve("dir").toFile();
    FileIOUtils.mkdir(dir);
    assertTrue(dir.exists());

    new File(dir, "t.txt").createNewFile();
    new File(dir, "subdir").mkdirs();
    new File(dir, "subdir" + File.pathSeparator + "z.txt").createNewFile();
    FileIOUtils.deleteDirectory(dir);
    assertFalse(dir.exists());
  }

  @Test
  public void testInputStreamReads() throws IOException {
    String msg = "hudi rocks!";
    ByteArrayInputStream inputStream = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    assertEquals(msg, FileIOUtils.readAsUTFString(inputStream));
    inputStream = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
    assertEquals(msg.length(), FileIOUtils.readAsByteArray(inputStream).length);
  }

  @Test
  public void testReadAsUTFStringLines() {
    String content = "a\nb\nc";
    List<String> expectedLines = Arrays.stream(new String[]{"a", "b", "c"}).collect(Collectors.toList());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedLines, FileIOUtils.readAsUTFStringLines(inputStream));
  }
}
