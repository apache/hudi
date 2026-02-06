/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.collection.RocksDBDAO;

import java.io.IOException;

/**
 * An implementation of {@link IndexBackend} based on RocksDB.
 */
public class RocksDBIndexBackend implements IndexBackend {
  private static final String COLUMN_FAMILY = "index_cache";

  private final RocksDBDAO rocksDBDAO;

  public RocksDBIndexBackend(String rocksDbBasePath) {
    this.rocksDBDAO = new RocksDBDAO("hudi-index-backend", rocksDbBasePath);
    this.rocksDBDAO.addColumnFamily(COLUMN_FAMILY);
  }

  @Override
  public HoodieRecordGlobalLocation get(String recordKey) {
    return this.rocksDBDAO.get(COLUMN_FAMILY, recordKey);
  }

  @Override
  public void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) {
    this.rocksDBDAO.put(COLUMN_FAMILY, recordKey, recordGlobalLocation);
  }

  @Override
  public void close() throws IOException {
    this.rocksDBDAO.close();
  }
}
