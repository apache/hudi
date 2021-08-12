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

package org.apache.hudi.index.dfs;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieSecondaryIndex;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collection;
import java.util.Map;

/**
 * Hoodie Index implementation backed by HBase.
 */
public class SparkFileToRangeIndex<T extends HoodieRecordPayload> extends HoodieSecondaryIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkFileToRangeIndex(HoodieWriteConfig config, HoodieEngineContext engineContext) {
    super(config, engineContext);
  }

  @Override
  public void updateIndex(final HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata,
                          final String instantTime,
                          final HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) throws HoodieIndexException {
    Map<String, Collection<HoodieColumnRangeMetadata<Comparable>>> fileToColumnRangeInfo = HoodieTableMetadataUtil.createRangeIndexInfoFromWriteStats(
        engineContext,
        hoodieTable.getMetaClient(), 
        writeMetadata.getWriteStatuses().map(WriteStatus::getStat).collect());
    SparkHoodieBackedTableMetadataWriter.create(hoodieTable.getHadoopConf(), getWriteConfig(), getEngineContext()).update(fileToColumnRangeInfo, instantTime);
  }

  @Override
  public boolean rollbackCommit(final String instantTime) {
    throw new HoodieException("rollback not supported yet on range index");
  }
}
