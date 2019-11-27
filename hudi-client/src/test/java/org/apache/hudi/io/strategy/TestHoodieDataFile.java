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

import org.apache.hudi.common.model.HoodieDataFile;

import java.util.UUID;

public class TestHoodieDataFile extends HoodieDataFile {

  private final long size;

  public TestHoodieDataFile(long size) {
    super("/tmp/XYXYXYXYXYYX_11_20180918020003.parquet");
    this.size = size;
  }

  public static HoodieDataFile newDataFile(long size) {
    return new TestHoodieDataFile(size);
  }

  @Override
  public String getPath() {
    return "/tmp/test";
  }

  @Override
  public String getFileId() {
    return UUID.randomUUID().toString();
  }

  @Override
  public String getCommitTime() {
    return "100";
  }

  @Override
  public long getFileSize() {
    return size;
  }
}
