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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieLogFile {
  private final String pathStr = "file:///tmp/hoodie/2021/01/01/.136281f3-c24e-423b-a65a-95dbfbddce1d_100.log.2_1-0-1";
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int logVersion = 2;
  private final String writeToken = "1-0-1";
  private final String fileExtension = "log";

  private final int length = 10;
  private final short blockReplication = 2;
  private final long blockSize = 1000000L;

  @Test
  void createFromLogFile() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathInfo);
    assertFileGetters(pathInfo, new HoodieLogFile(hoodieLogFile), length);
  }

  @Test
  void createFromFileStatus() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathInfo);
    assertFileGetters(pathInfo, hoodieLogFile, length);
  }

  @Test
  void createFromPath() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new StoragePath(pathStr));
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromPathAndLength() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new StoragePath(pathStr), length);
    assertFileGetters(null, hoodieLogFile, length);
  }

  @Test
  void createFromString() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathStr);
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromStringWithSuffix() {
    String suffix = ".cdc";
    String pathWithSuffix = pathStr + suffix;
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathWithSuffix);
    assertFileGetters(pathWithSuffix, null, hoodieLogFile, -1, suffix);
  }

  @Test
  void name() {
  }

  @Test
  void testLogPatternMatch() {
    boolean firstApproach = true; // toggle this to false if you want the other one
    int runs = 100;
    long totalTime = 0;
    Random random = new Random();

    Pattern LOG_FILE_PATTERN_1 =
        Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");
    Pattern LOG_FILE_PATTERN_2 =
        Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?$");

    for (int r = 0; r < runs; r++) {
      HoodieTimer timer = HoodieTimer.start();
      for (int i = 1; i < 1_000_000; i++) {
        String logFileName = generateLogFileName(random);
        if (firstApproach) {
          Matcher matcher = LOG_FILE_PATTERN_1.matcher(logFileName);
          assertTrue(matcher.find());
        } else {
          Matcher matcher = LOG_FILE_PATTERN_2.matcher(logFileName);
          assertTrue(matcher.matches());
        }
      }
      long elapsed = timer.endTimer();
      totalTime += elapsed;
      System.out.println("Run " + (r + 1) + " took " + elapsed + " ms");
    }

    System.out.println("===================================");
    System.out.println("Average time (" + (firstApproach ? "find" : "matches") + ") = "
        + (totalTime / runs) + " ms");
  }

  @Test
  void testLogPatternMatch_MisMatch() {
    boolean firstApproach = true; // toggle this to false if you want the other one
    int runs = 100;
    long totalTime = 0;
    Random random = new Random();

    Pattern LOG_FILE_PATTERN_1 =
        Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");
    Pattern LOG_FILE_PATTERN_2 =
        Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?$");

    for (int r = 0; r < runs; r++) {
      HoodieTimer timer = HoodieTimer.start();
      for (int i = 1; i < 1_000_000; i++) {
        String logFileName = randomString(random, 10, 100) + ".hfile";
        if (firstApproach) {
          Matcher matcher = LOG_FILE_PATTERN_1.matcher(logFileName);
          assertFalse(matcher.find());
        } else {
          Matcher matcher = LOG_FILE_PATTERN_2.matcher(logFileName);
          assertFalse(matcher.matches());
        }
      }
      long elapsed = timer.endTimer();
      totalTime += elapsed;
      System.out.println("Run " + (r + 1) + " took " + elapsed + " ms");
    }

    System.out.println("===================================");
    System.out.println("Average time (" + (firstApproach ? "find" : "matches") + ") = "
        + (totalTime / runs) + " ms");
  }

  private String generateLogFileName(Random random) {
    // Part 1: random name before underscore
    String logFileId = randomString(random, 3, 6);

    // Part 2: random name after underscore
    String instantId = String.valueOf(Math.abs(random.nextLong()));

    // Extension: log or archive
    String fileExtension = random.nextBoolean() ? ".log" : ".archive";

    // Random number
    int logVersion = Math.abs(random.nextInt()) % 10;

    // Sometimes include date + optional .cdc

    String logWriteToken = "";

    StringBuilder sb = new StringBuilder();

    int token_1 = Math.abs(random.nextInt());
    int token_2 = Math.abs(random.nextInt());
    int token_3 = Math.abs(random.nextInt());

    sb.append(token_1).append("-")
        .append(token_2).append("-")
        .append(token_3);

    if (random.nextBoolean()) {
      sb.append(".cdc");
    }

    logWriteToken = sb.toString();

    return FSUtils.makeLogFileName(logFileId, fileExtension, instantId, logVersion, logWriteToken);
  }

  private String randomString(Random random, int minLen, int maxLen) {
    int len = minLen + random.nextInt(maxLen - minLen + 1);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      char c = (char) ('a' + random.nextInt(26));
      sb.append(c);
    }
    return sb.toString();
  }

  private void assertFileGetters(StoragePathInfo pathInfo, HoodieLogFile hoodieLogFile,
                                 long fileLength) {
    assertFileGetters(pathStr, pathInfo, hoodieLogFile, fileLength, "");
  }

  private void assertFileGetters(String pathStr, StoragePathInfo pathInfo,
                                 HoodieLogFile hoodieLogFile,
                                 long fileLength, String suffix) {
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals(baseCommitTime, hoodieLogFile.getDeltaCommitTime());
    assertEquals(logVersion, hoodieLogFile.getLogVersion());
    assertEquals(writeToken, hoodieLogFile.getLogWriteToken());
    assertEquals(fileExtension, hoodieLogFile.getFileExtension());
    assertEquals(new StoragePath(pathStr), hoodieLogFile.getPath());
    assertEquals(fileLength, hoodieLogFile.getFileSize());
    assertEquals(pathInfo, hoodieLogFile.getPathInfo());
    assertEquals(suffix, hoodieLogFile.getSuffix());
  }
}
