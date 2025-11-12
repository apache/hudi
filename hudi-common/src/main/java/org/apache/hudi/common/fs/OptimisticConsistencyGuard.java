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

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * A consistency guard which sleeps for configured period of time only on APPEAR. It is a no-op for DISAPPEAR.
 * This is specifically for S3A filesystem and here is the rational.
 * This guard is used when deleting data files corresponding to marker files that needs to be deleted.
 * There are two tricky cases that needs to be considered. Case 1 : A data file creation is eventually consistent and hence
 * when issuing deletes, it may not be found. Case 2: a data file was never created in the first place since the process crashed.
 * In S3A, GET and LIST are eventually consistent, and delete() implementation internally does a LIST/EXISTS.
 * Prior to this patch, hudi was leveraging {@link FailSafeConsistencyGuard} which was doing the following to delete data files.
 * Step1: wait for all files to appear with linear backoff.
 * Step2: issue deletes
 * Step3: wait for all files to disappear with linear backoff.
 * Step1 and Step2 is handled by {@link FailSafeConsistencyGuard}.
 *
 * We are simplifying these steps with {@link OptimisticConsistencyGuard}.
 * Step1: Check if all files adhere to visibility event. If yes, proceed to Step 3.
 * Step2: If not, Sleep for a configured threshold and then proceed to next step.
 * Step3: issue deletes.
 *
 * With this, if any files that was created, should be available within configured threshold(eventual consistency).
 * Delete() will return false if FileNotFound. So, both cases are taken care of this {@link ConsistencyGuard}.
 */
public class OptimisticConsistencyGuard extends FailSafeConsistencyGuard {

  private static final Logger LOG = LoggerFactory.getLogger(OptimisticConsistencyGuard.class);

  public OptimisticConsistencyGuard(HoodieStorage storage,
                                    ConsistencyGuardConfig consistencyGuardConfig) {
    super(storage, consistencyGuardConfig);
  }

  @Override
  public void waitTillFileAppears(StoragePath filePath) throws TimeoutException {
    try {
      if (!checkFileVisibility(filePath, FileVisibility.APPEAR)) {
        Thread.sleep(consistencyGuardConfig.getOptimisticConsistencyGuardSleepTimeMs());
      }
    } catch (IOException | InterruptedException ioe) {
      LOG.warn("Got IOException or InterruptedException waiting for file visibility. Ignoring",
          ioe);
    }
  }

  @Override
  public void waitTillFileDisappears(StoragePath filePath) throws TimeoutException {
    // no op
  }

  @Override
  public void waitTillAllFilesAppear(String dirPath, List<String> files) throws TimeoutException {
    try {
      if (!checkFilesVisibility(1, new StoragePath(dirPath),
          getFilesWithoutSchemeAndAuthority(files), FileVisibility.APPEAR)) {
        Thread.sleep(consistencyGuardConfig.getOptimisticConsistencyGuardSleepTimeMs());
      }
    } catch (InterruptedException ie) {
      LOG.warn("Got InterruptedException waiting for file visibility. Ignoring", ie);
    }
  }

  @Override
  public void waitTillAllFilesDisappear(String dirPath, List<String> files) throws  TimeoutException {
    // no op
  }
}
