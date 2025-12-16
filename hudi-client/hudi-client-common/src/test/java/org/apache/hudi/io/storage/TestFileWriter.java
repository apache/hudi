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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.Properties;

public class TestFileWriter implements HoodieFileWriter {
  private boolean closed = false;
  private boolean failOnWrite = false;

  public TestFileWriter(StoragePath filePath, HoodieStorage storage, boolean failOnInitialization) throws IOException {
    this(filePath, storage, failOnInitialization, false);
  }

  public TestFileWriter(StoragePath filePath, HoodieStorage storage, boolean failOnInitialization, boolean failOnWrite) throws IOException {
    this.failOnWrite = failOnWrite;
    storage.create(filePath, false);
    if (failOnInitialization) {
      throw new IOException("Simulated file writer initialization failure");
    }
  }

  @Override
  public boolean canWrite() {
    return !closed;
  }

  @Override
  public void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    if (closed) {
      throw new IOException("Writer is closed");
    }
    if (failOnWrite) {
      throw new IOException("Simulated file writer write failure");
    }
  }

  @Override
  public void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    if (closed) {
      throw new IOException("Writer is closed");
    }
    if (failOnWrite) {
      throw new IOException("Simulated file writer write failure");
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
