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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is essentially same as Create Handle but overrides two things
 * 1) Schema : Metadata bootstrap writes only metadata fields as part of write. So, setup the writer schema accordingly.
 * 2) canWrite is overridden to always return true so that skeleton file and bootstrap file is aligned and we don't end up
 *    writing more than 1 skeleton file for the same bootstrap file.
 * @param <T> HoodieRecordPayload
 */
public class HoodieBootstrapHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  // NOTE: We have to use schema containing all the meta-fields in here b/c unlike for [[HoodieAvroRecord]],
  //       [[HoodieSparkRecord]] requires records to always bear either all or no meta-fields in the
  //       record schema (ie partial inclusion of the meta-fields in the schema is not allowed)
  public static final HoodieSchema METADATA_BOOTSTRAP_RECORD_SCHEMA = createMetadataBootstrapRecordSchema();

  public HoodieBootstrapHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    super(config, commitTime, hoodieTable, partitionPath, fileId,
        Option.of(METADATA_BOOTSTRAP_RECORD_SCHEMA), taskContextSupplier);
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }

  private static HoodieSchema createMetadataBootstrapRecordSchema() {
    List<HoodieSchemaField> fields =
        HoodieRecord.HOODIE_META_COLUMNS.stream()
            .map(metaField ->
                HoodieSchemaField.of(metaField, HoodieSchema.createNullable(HoodieSchemaType.STRING), "", HoodieSchema.NULL_VALUE))
            .collect(Collectors.toList());
    return HoodieSchema.createRecord("HoodieRecordKey", "", "", false, fields);
  }
}