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

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public interface HoodieSparkFileReader extends HoodieFileReader<InternalRow> {

  ClosableIterator<InternalRow> getInternalRowIterator(Schema readerSchema) throws IOException;

  default ClosableIterator<HoodieRecord<org.apache.spark.sql.catalyst.InternalRow>> getRecordIterator(Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException {
    mapper = (HoodieRecord.Mapper<InternalRow, InternalRow>) (internalRow) -> {
      return new HoodieSparkRecord(new HoodieKey(internalRow.getString(HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal()),
          internalRow.getString(HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.ordinal())),
          internalRow, null);
    };
    return new MappingIterator<>(getInternalRowIterator(readerSchema), mapper::apply);
  }
}
