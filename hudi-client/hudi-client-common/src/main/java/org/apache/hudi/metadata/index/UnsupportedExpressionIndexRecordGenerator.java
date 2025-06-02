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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;

import java.util.List;

public class UnsupportedExpressionIndexRecordGenerator implements ExpressionIndexRecordGenerator {
  private final EngineType engineType;

  public UnsupportedExpressionIndexRecordGenerator(EngineType engineType) {
    this.engineType = engineType;
  }

  @Override
  public EngineType getEngineType() {
    return engineType;
  }

  @Override
  public HoodieData<HoodieRecord> generate(
      List<FileToIndex> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism,
      Schema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime) {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieNotSupportedException("Table version 6 and below does not support expression index");
    }
    throw new HoodieNotSupportedException(engineType + " engine does not support building expression index yet");
  }
}
