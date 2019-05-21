/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A consistency checker that fails if it is unable to meet the required condition within a specified timeout
 */
public class FailSafeConsistencyGuard implements ConsistencyGuard {

  private static final transient Logger log = LogManager.getLogger(FailSafeConsistencyGuard.class);

  private final FileSystem fs;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final long maxDelayMs;

  public FailSafeConsistencyGuard(FileSystem fs, int maxAttempts, long initalDelayMs, long maxDelayMs) {
    this.fs = fs;
    this.maxAttempts = maxAttempts;
    this.initialDelayMs = initalDelayMs;
    this.maxDelayMs = maxDelayMs;
  }

  @Override
  public void waitTillFileAppears(Path filePath) throws TimeoutException {
    waitForFileVisibility(filePath,  FileVisibility.APPEAR);
  }

  @Override
  public void waitTillFileDisappears(Path filePath)
      throws TimeoutException {
    waitForFileVisibility(filePath, FileVisibility.DISAPPEAR);
  }

  @Override
  public void waitTillAllFilesAppear(String dirPath, List<String> files) throws TimeoutException {
    waitForFilesVisibility(dirPath, files, FileVisibility.APPEAR);
  }

  @Override
  public void waitTillAllFilesDisappear(String dirPath, List<String> files) throws TimeoutException {
    waitForFilesVisibility(dirPath, files, FileVisibility.DISAPPEAR);
  }

  /**
   * Helper function to wait for all files belonging to single directory to appear
   * @param dirPath Dir Path
   * @param files Files to appear/disappear
   * @param event Appear/Disappear
   * @throws TimeoutException
   */
  public void waitForFilesVisibility(String dirPath, List<String> files, FileVisibility event)
      throws TimeoutException {
    Path dir = new Path(dirPath);
    List<String> filesWithoutSchemeAndAuthority =
        files.stream().map(f -> Path.getPathWithoutSchemeAndAuthority(new Path(f))).map(p -> p.toString())
            .collect(Collectors.toList());

    retryTillSuccess((retryNum) -> {
      try {
        log.info("Trying " + retryNum);
        FileStatus[] entries = fs.listStatus(dir);
        List<String> gotFiles = Arrays.stream(entries).map(e -> Path.getPathWithoutSchemeAndAuthority(e.getPath()))
            .map(p -> p.toString()).collect(Collectors.toList());
        List<String> candidateFiles = new ArrayList<>(filesWithoutSchemeAndAuthority);
        boolean altered = candidateFiles.removeAll(gotFiles);

        switch (event) {
          case DISAPPEAR:
            log.info("Following files are visible" + candidateFiles);
            // If no candidate files gets removed, it means all of them have disappeared
            return !altered;
          case APPEAR:
          default:
            // if all files appear, the list is empty
            return candidateFiles.isEmpty();
        }
      } catch (IOException ioe) {
        log.warn("Got IOException waiting for file event. Have tried " + retryNum + " time(s)", ioe);
      }
      return false;
    }, "Timed out waiting for filles to become visible");
  }

  /**
   * Helper to check of file visibility
   * @param filePath File Path
   * @param visibility Visibility
   * @return
   * @throws IOException
   */
  private boolean checkFileVisibility(Path filePath, FileVisibility visibility) throws IOException {
    try {
      FileStatus[] status = fs.listStatus(filePath);
      switch (visibility) {
        case APPEAR:
          return status.length != 0;
        case DISAPPEAR:
        default:
          return status.length == 0;
      }
    } catch (FileNotFoundException nfe) {
      switch (visibility) {
        case APPEAR:
          return false;
        case DISAPPEAR:
        default:
          return true;
      }
    }
  }

  /**
   * Helper function to wait till file either appears/disappears
   * @param filePath File Path
   * @param visibility
   * @throws TimeoutException
   */
  private void waitForFileVisibility(Path filePath, FileVisibility visibility) throws TimeoutException {
    long waitMs = initialDelayMs;
    int attempt = 0;
    while (attempt < maxAttempts) {
      try {
        if (checkFileVisibility(filePath, visibility)) {
          return;
        }
      } catch (IOException ioe) {
        log.warn("Got IOException waiting for file visibility. Retrying", ioe);
      }

      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
      waitMs = waitMs > maxDelayMs ? maxDelayMs : waitMs;
      attempt++;
    }
    throw new TimeoutException("Timed-out waiting for the file to " + visibility.name());
  }

  /**
   * Retries the predicate for condfigurable number of times till we the predicate returns success
   * @param predicate Predicate Function
   * @param timedOutMessage Timed-Out message for logging
   * @throws TimeoutException when retries are exhausted
   */
  private void retryTillSuccess(Function<Integer, Boolean> predicate, String timedOutMessage) throws TimeoutException {
    long waitMs = initialDelayMs;
    int attempt = 0;
    log.warn("Max Attempts=" + maxAttempts);
    while (attempt < maxAttempts) {
      boolean success = predicate.apply(attempt);
      if (success) {
        return;
      }
      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
      waitMs = waitMs > maxDelayMs ? maxDelayMs : waitMs;
      attempt++;
    }
    throw new TimeoutException(timedOutMessage);

  }

  void sleepSafe(long waitMs) {
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      // ignore & continue next attempt
    }
  }
}
