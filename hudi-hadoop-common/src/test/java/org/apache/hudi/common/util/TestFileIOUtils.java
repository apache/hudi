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
import org.apache.hudi.io.util.FileIOUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
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
      org.apache.hudi.io.util.FileIOUtils.mkdir(tempDir.toFile());
    } catch (IOException e) {
      fail("Should not error out if dir exists already");
    }
    File dir = tempDir.resolve("dir").toFile();
    org.apache.hudi.io.util.FileIOUtils.mkdir(dir);
    assertTrue(dir.exists());

    new File(dir, "t.txt").createNewFile();
    new File(dir, "subdir").mkdirs();
    new File(dir, "subdir" + File.pathSeparator + "z.txt").createNewFile();
    org.apache.hudi.io.util.FileIOUtils.deleteDirectory(dir);
    assertFalse(dir.exists());
  }

  @Test
  public void testInputStreamReads() throws IOException {
    String msg = "hudi rocks!";
    ByteArrayInputStream inputStream = new ByteArrayInputStream(getUTF8Bytes(msg));
    assertEquals(msg, org.apache.hudi.io.util.FileIOUtils.readAsUTFString(inputStream));
    inputStream = new ByteArrayInputStream(getUTF8Bytes(msg));
    assertEquals(msg.length(), org.apache.hudi.io.util.FileIOUtils.readAsByteArray(inputStream).length);
  }

  @Test
  public void testReadAsUTFStringLines() {
    String content = "a\nb\nc";
    List<String> expectedLines = Arrays.stream(new String[] {"a", "b", "c"}).collect(Collectors.toList());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(getUTF8Bytes(content));
    assertEquals(expectedLines, org.apache.hudi.io.util.FileIOUtils.readAsUTFStringLines(inputStream));
  }
  
  @Test
  public void testGetConfiguredLocalDirs() {
    Map<String, String> env = System.getenv();
    Class<?> clazz = env.getClass();
    Map<String, String> envMaps = null;
    try {
      Field field = clazz.getDeclaredField("m");
      field.setAccessible(true);
      envMaps = (Map<String, String>) field.get(env);
      envMaps.put("CONTAINER_ID", "xxxxx");
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    assertEquals(String.join("", org.apache.hudi.io.util.FileIOUtils.getConfiguredLocalDirs()),
            System.getProperty("java.io.tmpdir"));
    envMaps.put("LOCAL_DIRS", "/xxx");
    assertEquals(String.join("", org.apache.hudi.io.util.FileIOUtils.getConfiguredLocalDirs()),
            envMaps.get("LOCAL_DIRS"));
  }

  @Test
  public void testGetDefaultSpillableMapBasePath() {
    // Store the original value of the system property, so we can reset it after the test
    String originalTmpDir = System.getProperty("java.io.tmpdir");

    // Case when local dirs provided
    System.setProperty("java.io.tmpdir", "dir1,dir2,dir3");
    String result = org.apache.hudi.io.util.FileIOUtils.getDefaultSpillableMapBasePath();
    assertTrue(result.equals("dir1") || result.equals("dir2") || result.equals("dir3"));

    // Clear the property for the next case
    System.clearProperty("java.io.tmpdir");

    // Case when local dirs not provided
    result = FileIOUtils.getDefaultSpillableMapBasePath();
    assertEquals("/tmp/", result);

    // Reset the original value
    if (originalTmpDir != null) {
      System.setProperty("java.io.tmpdir", originalTmpDir);
    } else {
      System.clearProperty("java.io.tmpdir");
    }
  }
}
