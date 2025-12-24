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

package org.apache.hudi.io.lsm;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.collection.MappingIterator;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;

public class SparkRecordReader implements RecordReader<HoodieRecord> {
  private final StructType structType;
  private final int recordKeyIndex;
  private Iterator<InternalRow> iterator;

  public SparkRecordReader(Iterator<InternalRow> iterator, Schema readSchema) {
    this.iterator = iterator;
    this.structType = HoodieInternalRowUtils.getCachedSchema(readSchema);
    this.recordKeyIndex = (int)structType.getFieldIndex(HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName()).get();
  }

  @Override
  public Iterator<HoodieRecord> read() throws IOException {
    return new MappingIterator<>(iterator, record -> {
      return new HoodieSparkRecord(record.getUTF8String(recordKeyIndex), record, structType);
    });
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
