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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A consistency checker that fails if it is unable to meet the required condition within a specified timeout.
 */
public class FailSafeConsistencyGuard implements ConsistencyGuard {

  private static final Logger LOG = LoggerFactory.getLogger(FailSafeConsistencyGuard.class);

  private final FileSystem fs;
  private final ConsistencyGuardConfig consistencyGuardConfig;

  public FailSafeConsistencyGuard(FileSystem fs, ConsistencyGuardConfig consistencyGuardConfig) {
    this.fs = fs;
    this.consistencyGuardConfig = consistencyGuardConfig;
    Preconditions.checkArgument(consistencyGuardConfig.isConsistencyCheckEnabled());
  }

  @Override
  public void waitTillFileAppears(Path filePath) throws TimeoutException {
    waitForFileVisibility(filePath, FileVisibility.APPEAR);
  }

  @Override
  public void waitTillFileDisappears(Path filePath) throws TimeoutException {
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
   * Helper function to wait for all files belonging to single directory to appear.
   * 
   * @param dirPath Dir Path
   * @param files Files to appear/disappear
   * @param event Appear/Disappear
   * @throws TimeoutException
   */
  public void waitForFilesVisibility(String dirPath, List<String> files, FileVisibility event) throws TimeoutException {
    Path dir = new Path(dirPath);
    List<String> filesWithoutSchemeAndAuthority =
        files.stream().map(f -> Path.getPathWithoutSchemeAndAuthority(new Path(f))).map(p -> p.toString())
            .collect(Collectors.toList());

    retryTillSuccess((retryNum) -> {
      try {
        LOG.info("Trying {}", retryNum);
        FileStatus[] entries = fs.listStatus(dir);
        List<String> gotFiles = Arrays.stream(entries).map(e -> Path.getPathWithoutSchemeAndAuthority(e.getPath()))
            .map(p -> p.toString()).collect(Collectors.toList());
        List<String> candidateFiles = new ArrayList<>(filesWithoutSchemeAndAuthority);
        boolean altered = candidateFiles.removeAll(gotFiles);

        switch (event) {
          case DISAPPEAR:
            LOG.info("Following files are visible{}", candidateFiles);
            // If no candidate files gets removed, it means all of them have disappeared
            return !altered;
          case APPEAR:
          default:
            // if all files appear, the list is empty
            return candidateFiles.isEmpty();
        }
      } catch (IOException ioe) {
        LOG.warn("Got IOException waiting for file event. Have tried {} time(s)", retryNum, ioe);
      }
      return false;
    }, "Timed out waiting for files to become visible");
  }

  /**
   * Helper to check of file visibility.
   * 
   * @param filePath File Path
   * @param visibility Visibility
   * @return
   * @throws IOException
   */
  private boolean checkFileVisibility(Path filePath, FileVisibility visibility) throws IOException {
    try {
      FileStatus status = fs.getFileStatus(filePath);
      switch (visibility) {
        case APPEAR:
          return status != null;
        case DISAPPEAR:
        default:
          return status == null;
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
   * Helper function to wait till file either appears/disappears.
   * 
   * @param filePath File Path
   * @param visibility
   * @throws TimeoutException
   */
  private void waitForFileVisibility(Path filePath, FileVisibility visibility) throws TimeoutException {
    long waitMs = consistencyGuardConfig.getInitialConsistencyCheckIntervalMs();
    int attempt = 0;
    while (attempt < consistencyGuardConfig.getMaxConsistencyChecks()) {
      try {
        if (checkFileVisibility(filePath, visibility)) {
          return;
        }
      } catch (IOException ioe) {
        LOG.warn("Got IOException waiting for file visibility. Retrying", ioe);
      }

      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
      waitMs = Math.min(waitMs, consistencyGuardConfig.getMaxConsistencyCheckIntervalMs());
      attempt++;
    }
    throw new TimeoutException("Timed-out waiting for the file to " + visibility.name());
  }

  /**
   * Retries the predicate for condfigurable number of times till we the predicate returns success.
   * 
   * @param predicate Predicate Function
   * @param timedOutMessage Timed-Out message for logging
   * @throws TimeoutException when retries are exhausted
   */
  private void retryTillSuccess(Function<Integer, Boolean> predicate, String timedOutMessage) throws TimeoutException {
    long waitMs = consistencyGuardConfig.getInitialConsistencyCheckIntervalMs();
    int attempt = 0;
    LOG.info("Max Attempts={}", consistencyGuardConfig.getMaxConsistencyChecks());
    while (attempt < consistencyGuardConfig.getMaxConsistencyChecks()) {
      boolean success = predicate.apply(attempt);
      if (success) {
        return;
      }
      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
      waitMs = Math.min(waitMs, consistencyGuardConfig.getMaxConsistencyCheckIntervalMs());
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
