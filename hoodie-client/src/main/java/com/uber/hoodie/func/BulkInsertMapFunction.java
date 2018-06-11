/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.Function2;


/**
 * Map function that handles a sorted stream of HoodieRecords
 */
public class BulkInsertMapFunction<T extends HoodieRecordPayload> implements
    Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<List<WriteStatus>>> {

  private String commitTime;
  private HoodieWriteConfig config;
  private HoodieTable<T> hoodieTable;

  public BulkInsertMapFunction(String commitTime, HoodieWriteConfig config,
      HoodieTable<T> hoodieTable) {
    this.commitTime = commitTime;
    this.config = config;
    this.hoodieTable = hoodieTable;
  }

  @Override
  public Iterator<List<WriteStatus>> call(Integer partition,
      Iterator<HoodieRecord<T>> sortedRecordItr) throws Exception {
    return new CopyOnWriteLazyInsertIterable<>(sortedRecordItr, config, commitTime, hoodieTable);
  }
}
