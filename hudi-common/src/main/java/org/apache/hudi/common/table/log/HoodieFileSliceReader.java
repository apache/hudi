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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class HoodieFileSliceReader<T> extends LogFileIterator<T> {
  private Option<Iterator<HoodieRecord>> baseFileIterator;
  private HoodieMergedLogRecordScanner scanner;
  private Schema schema;
  private Properties props;

  private TypedProperties payloadProps = new TypedProperties();
  private Option<Pair<String, String>> simpleKeyGenFieldsOpt;
  HoodieRecordMerger merger;

  public HoodieFileSliceReader(Option<HoodieFileReader> baseFileReader,
                                   HoodieMergedLogRecordScanner scanner, Schema schema, String preCombineField, HoodieRecordMerger merger,
                               Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt) throws IOException {
    super(scanner);
    if (baseFileReader.isPresent()) {
      this.baseFileIterator = Option.of(baseFileReader.get().getRecordIterator(schema));
    } else {
      this.baseFileIterator = Option.empty();
    }
    this.scanner = scanner;
    this.schema = schema;
    this.merger = merger;
    if (preCombineField != null) {
      payloadProps.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, preCombineField);
    }
    this.props = props;
    this.simpleKeyGenFieldsOpt = simpleKeyGenFieldsOpt;
  }

  private boolean hasNextInternal() {
    while (baseFileIterator.isPresent() && baseFileIterator.get().hasNext()) {
      try {
        HoodieRecord currentRecord = baseFileIterator.get().next().wrapIntoHoodieRecordPayloadWithParams(schema, props,
            simpleKeyGenFieldsOpt, scanner.isWithOperationField(), scanner.getPartitionNameOverride(), false, Option.empty());

        if (!scanner.hasKey(currentRecord.getRecordKey())) {
          nextRecord = currentRecord;
          return true;
        }

        // Option<HoodieRecord> logRecord = removeLogRecord(currentRecord.getRecordKey());
        Option<HoodieRecord> logRecord = removeLogRecord(currentRecord);
        if (!logRecord.isPresent()) {
          nextRecord = currentRecord;
          return true;
        }
        Option<Pair<HoodieRecord, Schema>> mergedRecordOpt =  merger.merge(currentRecord, schema, logRecord.get(), schema, payloadProps);
        if (mergedRecordOpt.isPresent()) {
          HoodieRecord<T> mergedRecord = (HoodieRecord<T>) mergedRecordOpt.get().getLeft();
          nextRecord = mergedRecord.wrapIntoHoodieRecordPayloadWithParams(schema, props, simpleKeyGenFieldsOpt, scanner.isWithOperationField(),
              scanner.getPartitionNameOverride(), false, Option.empty());
          return true;
        }
      } catch (IOException e) {
        throw new HoodieClusteringException("Failed to wrapIntoHoodieRecordPayloadWithParams: " + e.getMessage());
      }
    }
    return super.doHasNext();
  }

  @Override
  protected boolean doHasNext() {
    return hasNextInternal();
  }

}
