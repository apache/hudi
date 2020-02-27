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

package org.apache.hudi.io.strategy;

import org.apache.hudi.common.model.HoodieLogFile;

import org.apache.hadoop.fs.Path;

public class TestHoodieLogFile extends HoodieLogFile {

  private final long size;

  public TestHoodieLogFile(long size) {
    super("/tmp/.ce481ee7-9e53-4a2e-9992-f9e295fa79c0_20180919184844.log.1");
    this.size = size;
  }

  public static HoodieLogFile newLogFile(long size) {
    return new TestHoodieLogFile(size);
  }

  @Override
  public Path getPath() {
    return new Path("/tmp/test-log");
  }

  @Override
  public long getFileSize() {
    return size;
  }
}
