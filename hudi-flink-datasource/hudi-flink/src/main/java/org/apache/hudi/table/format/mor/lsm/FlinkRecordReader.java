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

package org.apache.hudi.table.format.mor.lsm;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.io.lsm.RecordReader;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;

public class FlinkRecordReader implements RecordReader<HoodieRecord> {

  private final ClosableIterator<RowData> iterator;
  private RowDataSerializer rowDataSerializer;
  private final int recordKeyIndex;

  public FlinkRecordReader(ClosableIterator<RowData> iterator, RowDataSerializer rowDataSerializer, int recordKeyIndex) {
    this.iterator = iterator;
    this.rowDataSerializer = rowDataSerializer;
    this.recordKeyIndex = recordKeyIndex;
  }

  public FlinkRecordReader(ClosableIterator<RowData> iterator, int recordKeyIndex) {
    this.iterator = iterator;
    this.recordKeyIndex = recordKeyIndex;
  }
  
  @Nullable
  @Override
  public Iterator<HoodieRecord> read() throws IOException {
    return new MappingIterator<>(iterator, record -> {
      return new HoodieFlinkRecord(record.getString(recordKeyIndex), record);
    });
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
