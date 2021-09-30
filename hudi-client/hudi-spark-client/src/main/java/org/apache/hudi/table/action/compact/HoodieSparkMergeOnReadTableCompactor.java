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

package org.apache.hudi.table.action.compact;

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaRDD;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieSparkMergeOnReadTableCompactor<T extends HoodieRecordPayload>
    extends HoodieCompactor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  @Override
  public Schema getReaderSchema(HoodieWriteConfig config) {
    return HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
  }

  @Override
  public void updateReaderSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);

    // Here we firstly use the table schema as the reader schema to read
    // log file.That is because in the case of MergeInto, the config.getSchema may not
    // the same with the table schema.
    try {
      Schema readerSchema = schemaUtil.getTableAvroSchema(false);
      config.setSchema(readerSchema.toString());
    } catch (Exception e) {
      // If there is no commit in the table, just ignore the exception.
    }
  }

  @Override
  public void checkCompactionTimeline(
      HoodieTable table, String compactionInstantTime, AbstractHoodieWriteClient writeClient) {
    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    if (!pendingCompactionTimeline.containsInstant(instant)) {
      throw new IllegalStateException(
          "No Compaction request available at " + compactionInstantTime + " to run compaction");
    }
  }
}
