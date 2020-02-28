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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.CopyOnWriteInsertHandler;
import org.apache.hudi.execution.LazyInsertIterable.HoodieInsertValueGenResult;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BulkInsertMapFunctionForNonSortedRecords<T extends HoodieRecordPayload>
    extends BulkInsertMapFunction<T> {

  Map<String, CopyOnWriteInsertHandler> parallelWritersMap;

  public BulkInsertMapFunctionForNonSortedRecords(String commitTime,
      HoodieWriteConfig config,
      HoodieTable<T> hoodieTable,
      List<String> fileIDPrefixes) {
    super(commitTime, config, hoodieTable, fileIDPrefixes);
    parallelWritersMap = new HashMap<>();
  }

  @Override
  public Iterator<List<WriteStatus>> call(Integer partition,
      Iterator<HoodieRecord<T>> nonSortedRecordItr) {
    String idPrefix = fileIDPrefixes.get(partition);
    Schema schema = new Schema.Parser().parse(config.getSchema());
    HoodieRecord<T> hoodieRecord;
    while (nonSortedRecordItr.hasNext()) {
      hoodieRecord = nonSortedRecordItr.next();
      if (!parallelWritersMap.containsKey(hoodieRecord.getPartitionPath())) {
        parallelWritersMap.put(
            hoodieRecord.getPartitionPath(),
            new CopyOnWriteInsertHandler(config, instantTime, hoodieTable, idPrefix,
                hoodieTable.getSparkTaskContextSupplier(), new CreateHandleFactory<>()));
      }
      parallelWritersMap.get(hoodieRecord.getPartitionPath()).consumeOneRecord(
          new HoodieInsertValueGenResult(hoodieRecord, schema));
    }

    List<List<WriteStatus>> statuses = new ArrayList<>();
    for (CopyOnWriteInsertHandler handler : parallelWritersMap.values()) {
      handler.finish();
      statuses.add(handler.getResult());
    }
    return statuses.iterator();
  }
}
