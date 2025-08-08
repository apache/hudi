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

package org.apache.hudi.avro;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class HoodieAvroBytesReaderContext extends HoodieReaderContext<byte[]> {

  @Override
  public ClosableIterator<byte[]> getFileRecordIterator(StoragePath filePath,
                                                        long start,
                                                        long length,
                                                        Schema dataSchema,
                                                        Schema requiredSchema,
                                                        HoodieStorage storage) throws IOException {
    return null;
  }

  @Override
  public byte[] convertAvroRecord(IndexedRecord avroRecord) {
    return new byte[0];
  }

  @Override
  public GenericRecord convertToAvroRecord(byte[] record, Schema schema) {
    return null;
  }

  @Nullable
  @Override
  public byte[] getDeleteRow(byte[] record, String recordKey) {
    return new byte[0];
  }

  @Override
  protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    return null;
  }

  @Override
  public Object getValue(byte[] record, Schema schema, String fieldName) {
    return null;
  }

  @Override
  public String getMetaFieldValue(byte[] record, int pos) {
    return null;
  }

  @Override
  public HoodieRecord<byte[]> constructHoodieRecord(BufferedRecord<byte[]> bufferedRecord) {
    return null;
  }

  @Override
  public byte[] constructEngineRecord(Schema schema, Map<Integer, Object> updateValues, BufferedRecord<byte[]> baseRecord) {
    return new byte[0];
  }

  @Override
  public byte[] seal(byte[] record) {
    return new byte[0];
  }

  @Override
  public byte[] toBinaryRow(Schema avroSchema, byte[] record) {
    return new byte[0];
  }

  @Override
  public ClosableIterator<byte[]> mergeBootstrapReaders(ClosableIterator<byte[]> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<byte[]> dataFileIterator,
                                                        Schema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues) {
    return null;
  }

  @Override
  public UnaryOperator<byte[]> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    return null;
  }
}
