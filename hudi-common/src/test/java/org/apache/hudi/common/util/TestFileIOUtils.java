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

import org.apache.hudi.common.HoodieCommonTestHarness;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests file I/O utils.
 */
public class TestFileIOUtils extends HoodieCommonTestHarness {

  @Test
  public void testMkdirAndDelete() throws IOException {
    try {
      FileIOUtils.mkdir(folder.getRoot());
    } catch (IOException e) {
      fail("Should not error out if dir exists already");
    }
    File dir = new File(folder.getRoot().getAbsolutePath() + "/dir");
    FileIOUtils.mkdir(dir);
    assertTrue(dir.exists());

    new File(dir, "t.txt").createNewFile();
    new File(dir, "subdir").mkdirs();
    new File(dir, "subdir/z.txt").createNewFile();
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
}
