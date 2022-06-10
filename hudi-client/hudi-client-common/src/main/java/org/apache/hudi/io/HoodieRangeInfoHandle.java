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

package org.apache.hudi.io;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;

/**
 * Extract range information for a given file slice.
 */
public class HoodieRangeInfoHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  public HoodieRangeInfoHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
      Pair<String, String> partitionPathFilePair) {
    super(config, hoodieTable, partitionPathFilePair);
  }

  public String[] getMinMaxKeys() throws IOException {
    try (HoodieAvroFileReader reader = createNewFileReader()) {
      return reader.readMinMaxRecordKeys();
    }
  }
}
