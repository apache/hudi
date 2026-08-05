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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A consistency checker that fails if it is unable to meet the required condition within a specified timeout.
 */
public class FailSafeConsistencyGuard implements ConsistencyGuard {

  private static final Logger LOG = LoggerFactory.getLogger(FailSafeConsistencyGuard.class);

  protected final HoodieStorage storage;
  protected final ConsistencyGuardConfig consistencyGuardConfig;

  public FailSafeConsistencyGuard(HoodieStorage storage,
                                  ConsistencyGuardConfig consistencyGuardConfig) {
    this.storage = storage;
    this.consistencyGuardConfig = consistencyGuardConfig;
    ValidationUtils.checkArgument(consistencyGuardConfig.isConsistencyCheckEnabled());
  }

  @Override
  public void waitTillFileAppears(StoragePath filePath) throws TimeoutException {
    waitForFileVisibility(filePath, FileVisibility.APPEAR);
  }

  @Override
  public void waitTillFileDisappears(StoragePath filePath) throws TimeoutException {
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
    StoragePath dir = new StoragePath(dirPath);
    List<String> filesWithoutSchemeAndAuthority = getFilesWithoutSchemeAndAuthority(files);
    retryTillSuccess(dir, filesWithoutSchemeAndAuthority, event);
  }

  /**
   * Helper to check of file visibility.
   *
   * @param filePath   File Path
   * @param visibility Visibility
   * @return true (if file visible in Path), false (otherwise)
   * @throws IOException -
   */
  protected boolean checkFileVisibility(StoragePath filePath, FileVisibility visibility)
      throws IOException {
    try {
      StoragePathInfo pathInfo = storage.getPathInfo(filePath);
      switch (visibility) {
        case APPEAR:
          return pathInfo != null;
        case DISAPPEAR:
        default:
          return pathInfo == null;
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
   */
  private void waitForFileVisibility(StoragePath filePath, FileVisibility visibility)
      throws TimeoutException {
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
   * Retries the predicate for configurable number of times till we the predicate returns success.
   *
   * @param dir   directory of interest in which list of files are checked for visibility
   * @param files List of files to check for visibility
   * @param event {@link ConsistencyGuard.FileVisibility} event of interest.
   * @throws TimeoutException when retries are exhausted
   */
  private void retryTillSuccess(StoragePath dir, List<String> files, FileVisibility event)
      throws TimeoutException {
    long waitMs = consistencyGuardConfig.getInitialConsistencyCheckIntervalMs();
    int attempt = 0;
    LOG.info("Max Attempts=" + consistencyGuardConfig.getMaxConsistencyChecks());
    while (attempt < consistencyGuardConfig.getMaxConsistencyChecks()) {
      boolean success = checkFilesVisibility(attempt, dir, files, event);
      if (success) {
        return;
      }
      sleepSafe(waitMs);
      waitMs = waitMs * 2; // double check interval every attempt
      waitMs = Math.min(waitMs, consistencyGuardConfig.getMaxConsistencyCheckIntervalMs());
      attempt++;
    }
    throw new TimeoutException("Timed out waiting for files to adhere to event " + event.name());
  }

  /**
   * Helper to check for file visibility based on {@link ConsistencyGuard.FileVisibility} event.
   *
   * @param retryNum retry attempt count.
   * @param dir      directory of interest in which list of files are checked for visibility
   * @param files    List of files to check for visibility
   * @param event    {@link ConsistencyGuard.FileVisibility} event of interest.
   * @return {@code true} if condition succeeded. else {@code false}.
   */
  protected boolean checkFilesVisibility(int retryNum, StoragePath dir, List<String> files,
                                         FileVisibility event) {
    try {
      LOG.info("Trying " + retryNum);
      List<StoragePathInfo> entries = storage.listDirectEntries(dir);
      List<String> gotFiles = entries.stream()
          .map(e -> e.getPath().getPathWithoutSchemeAndAuthority())
          .map(StoragePath::toString).collect(Collectors.toList());
      List<String> candidateFiles = new ArrayList<>(files);
      boolean altered = candidateFiles.removeAll(gotFiles);

      switch (event) {
        case DISAPPEAR:
          LOG.info("Following files are visible" + candidateFiles);
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
  }

  /**
   * Generate file names without scheme and authority.
   *
   * @param files list of files of interest.
   * @return the filenames without scheme and authority.
   */
  protected List<String> getFilesWithoutSchemeAndAuthority(List<String> files) {
    return files.stream()
        .map(f -> new StoragePath(f).getPathWithoutSchemeAndAuthority())
        .map(StoragePath::toString)
        .collect(Collectors.toList());
  }

  private void sleepSafe(long waitMs) {
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      // ignore & continue next attempt
    }
  }
}
