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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class HoodieLogFileIterator implements ClosableIterator<HoodieRecord> {

  private final RealtimeSplit split;
  private final JobConf jobConf;
  private final HoodieFileReader baseFileReader;
  private final Iterator<HoodieRecord> baseFileIterator;
  private final Map<String, HoodieRecord> logRecords;
  private final Properties payloadProps = new Properties();
  private final Schema dataSchema;
  private final Schema readerSchema;
  private final HoodieRecordMerger recordMerger;
  private final int recordKeyIndex;
  private final HoodieTableMetaClient metaClient;

  private HoodieRecord record;
  private Lazy<Iterator<Option<HoodieRecord>>> logRecordsIterator;

  public HoodieLogFileIterator(RealtimeSplit split,
                               JobConf jobConf,
                               HoodieFileReader baseFileReader,
                               Map<String, HoodieRecord> logRecords,
                               Schema dataSchema,
                               Schema readerSchema) {
    this.split = split;
    this.jobConf = jobConf;
    this.baseFileReader = baseFileReader;
    this.logRecords = logRecords;
    this.metaClient = HoodieTableMetaClient.builder().setConf(jobConf).setBasePath(split.getBasePath()).build();
    setPayloadProperties(metaClient);
    this.dataSchema = dataSchema;
    this.readerSchema = readerSchema;
    this.recordMerger = HoodieRecordUtils.loadRecordMerger(HoodieHiveRecordMerger.class.getName());
    this.recordKeyIndex = split.getVirtualKeyInfo()
        .map(HoodieVirtualKeyInfo::getRecordKeyFieldIndex)
        .orElse(HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
    this.logRecordsIterator = Lazy.lazily(() -> logRecords.values().stream().map(Option::of).iterator());
    try {
      this.baseFileIterator = baseFileReader.getRecordIterator(readerSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get base records iterator.", e);
    }
  }

  public HoodieLogFileIterator(RealtimeSplit split,
                               JobConf jobConf,
                               HoodieFileReader baseFileReader,
                               Map<String, HoodieRecord> logRecords,
                               Schema dataSchema,
                               Schema readerSchema,
                               HoodieTableMetaClient metaClient) {
    this.split = split;
    this.jobConf = jobConf;
    this.baseFileReader = baseFileReader;
    this.logRecords = logRecords;
    this.metaClient = metaClient;
    setPayloadProperties(metaClient);
    this.dataSchema = dataSchema;
    this.readerSchema = readerSchema;
    this.recordMerger = HoodieRecordUtils.loadRecordMerger(HoodieHiveRecordMerger.class.getName());
    this.recordKeyIndex = split.getVirtualKeyInfo()
        .map(HoodieVirtualKeyInfo::getRecordKeyFieldIndex)
        .orElse(HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
    this.logRecordsIterator = Lazy.lazily(() -> logRecords.values().stream().map(Option::of).iterator());
    try {
      this.baseFileIterator = baseFileReader.getRecordIterator(readerSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get base records iterator.", e);
    }
  }

  private void setPayloadProperties(HoodieTableMetaClient metaClient) {
    if (metaClient.getTableConfig().getPreCombineField() != null) {
      this.payloadProps.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, metaClient.getTableConfig().getPreCombineField());
    }
  }

  @Override
  public boolean hasNext() {
    return hasNextInternal();
  }

  private boolean hasNextInternal() {
    if (baseFileIterator.hasNext()) {
      HoodieRecord baseRecord = baseFileIterator.next();
      String curKey = baseRecord.getKey().getRecordKey();
      Option<HoodieRecord> updatedRecordOpt = removeLogRecord(curKey);

      if (!updatedRecordOpt.isPresent()) {
        // No merge is required, simply load current row and project into required schema
        record = baseRecord;
        return true;
      }

      try {
        Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.merge(baseRecord, baseFileReader.getSchema(), updatedRecordOpt.get(), readerSchema, new TypedProperties(payloadProps));
        record = mergedRecord.map(Pair::getLeft).orElse(new HoodieEmptyRecord(baseRecord.getKey(), baseRecord.getRecordType()));
        return true;
      } catch (IOException e) {
        throw new HoodieException("Failed to merge");
      }
    }

    return false;
  }

  @Override
  public HoodieRecord next() {
    return record;
  }

  @Override
  public void close() {

  }

  private Option<HoodieRecord> removeLogRecord(String key) {
    return Option.ofNullable(logRecords.remove(key));
  }
}
