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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HoodieFileSliceCompactedReader<T> {

  private final Option<HoodieFileReader> baseFileReaderOpt;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final Map<String, HoodieRecord> deltaRecordMap;
  private final Set<String> deltaRecordKeys;
  private final HoodieRecordMerger recordMerger;
  private final HoodieWriteConfig config;
  private final HoodieTableMetaClient metaClient;
  // Schema handles
  private Schema readerSchema;
  private Schema writeSchema;
  private Schema writeSchemaWithMetaFields;

  public HoodieFileSliceCompactedReader(Option<HoodieFileReader> baseFileReader, HoodieMergedLogRecordScanner scanner, HoodieWriteConfig config) {
    this.baseFileReaderOpt = baseFileReader;
    this.mergedLogRecordScanner = scanner;
    this.deltaRecordMap = scanner.getRecords();
    this.deltaRecordKeys = new HashSet<>(this.deltaRecordMap.keySet());
    this.recordMerger = config.getRecordMerger();
    this.config = config;
    this.readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    this.writeSchema = new Schema.Parser().parse(config.getWriteSchema());
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    this.metaClient = scanner.hoodieTableMetaClient;
  }

  public List<HoodieRecord<T>> getCompactedRecords() throws IOException {
    List<HoodieRecord<T>> compactedRecords = new ArrayList<>();
    if (baseFileReaderOpt.isPresent()) {
      HoodieFileReader<T> baseFileReader = baseFileReaderOpt.get();
      ClosableIterator<HoodieRecord<T>> baseFileItr = baseFileReader.getRecordIterator(readerSchema);
      HoodieTableConfig tableConfig = metaClient.getTableConfig();
      Option<Pair<String, String>> simpleKeyGenFieldsOpt =
          tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
      while (baseFileItr.hasNext()) {
        HoodieRecord<T> record = baseFileItr.next().wrapIntoHoodieRecordPayloadWithParams(readerSchema,
            tableConfig.getProps(), simpleKeyGenFieldsOpt, mergedLogRecordScanner.isWithOperationField(), mergedLogRecordScanner.getPartitionNameOverride(), false);
        String key = record.getRecordKey();
        if (deltaRecordMap.containsKey(key)) {
          deltaRecordKeys.remove(key);
          Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(record.copy(), writeSchemaWithMetaFields, deltaRecordMap.get(key), writeSchemaWithMetaFields, config.getProps());
          if (!mergeResult.isPresent()) {
            continue;
          }
          compactedRecords.add(mergeResult.get().getLeft());
        } else {
          compactedRecords.add(record.copy());
        }
      }
    }
    for (String key : deltaRecordKeys) {
      compactedRecords.add(deltaRecordMap.get(key));
    }
    return compactedRecords;
  }
}
