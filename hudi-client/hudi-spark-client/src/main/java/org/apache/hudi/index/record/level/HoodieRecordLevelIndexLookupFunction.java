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

package org.apache.hudi.index.record.level;

import org.apache.hudi.avro.model.HoodieRecordLevelIndexRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.Iterator;

import scala.Tuple2;

public class HoodieRecordLevelIndexLookupFunction<T extends HoodieRecordPayload> implements
    Function2<Integer, Iterator<Tuple2<HoodieKey, HoodieRecord<T>>>, Iterator<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>>> {

  private final HoodieWriteConfig recordLevelIndexWriteConfig;
  private final HoodieWriteConfig datasetWriteConfig;
  private final HoodieTableMetaClient datasetMetaClient;
  private final HoodieTableMetaClient indexMetaClient;
  private final SerializableConfiguration serializableConfiguration;

  public HoodieRecordLevelIndexLookupFunction(HoodieWriteConfig writeConfig, HoodieWriteConfig recordLevelIndexWriteConfig,
      HoodieTableMetaClient datasetMetaClient, SerializableConfiguration serializableConfiguration) throws IOException {
    this.datasetWriteConfig = writeConfig;
    this.recordLevelIndexWriteConfig = recordLevelIndexWriteConfig;
    this.datasetMetaClient = datasetMetaClient;
    this.indexMetaClient = HoodieTableMetaClient.initTableType(serializableConfiguration.get(), recordLevelIndexWriteConfig.getBasePath(),
        HoodieTableType.MERGE_ON_READ, recordLevelIndexWriteConfig.getTableName(), "archived", HoodieRecordLevelIndexRecord.class.getName(),
        HoodieFileFormat.HFILE.toString());
    this.serializableConfiguration = serializableConfiguration;
  }

  @Override
  public Iterator<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>> call(Integer partitionIndex, Iterator<Tuple2<HoodieKey, HoodieRecord<T>>> recordsToLookup) throws Exception {
    return new RecordLevelIndexLazyLookupIterator(recordsToLookup, datasetWriteConfig, recordLevelIndexWriteConfig, datasetMetaClient, indexMetaClient, serializableConfiguration, partitionIndex);
  }
}


