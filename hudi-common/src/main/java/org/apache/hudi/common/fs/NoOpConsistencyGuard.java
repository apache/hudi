/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.fs;

import org.apache.hudi.storage.StoragePath;

import java.util.List;

/**
 * Default Consistency guard that does nothing. Used for lake storage which provided read-after-write
 * guarantees.
 */
public class NoOpConsistencyGuard implements ConsistencyGuard {

  @Override
  public void waitTillFileAppears(StoragePath filePath) {
  }

  @Override
  public void waitTillFileDisappears(StoragePath filePath) {
  }

  @Override
  public void waitTillAllFilesAppear(String dirPath, List<String> files) {

  }

  @Override
  public void waitTillAllFilesDisappear(String dirPath, List<String> files) {

  }
}
