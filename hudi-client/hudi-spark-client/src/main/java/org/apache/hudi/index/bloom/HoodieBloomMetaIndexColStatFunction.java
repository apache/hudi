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

package org.apache.hudi.index.bloom;

import org.apache.hudi.avro.model.HoodieColumnStats;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the actual files.
 */
public class HoodieBloomMetaIndexColStatFunction
    implements FlatMapFunction<Iterator<Tuple2<Tuple2<String, String>, HoodieKey>>, Tuple2<Tuple2<String, String>,
    HoodieKey>> {

  private static final Logger LOG = LogManager.getLogger(HoodieBloomMetaIndexColStatFunction.class);
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig config;

  public HoodieBloomMetaIndexColStatFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> call(
      Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> tuple2Iterator) throws Exception {
    Map<String, String> columnKeyHashToFileNameMap = new HashMap<>();
    Map<String, List<HoodieKey>> columnKeyHashToHoodieKeysMap = new HashMap<>();
    List<Tuple2<Tuple2<String, String>, HoodieKey>> result = new ArrayList<>();

    while (tuple2Iterator.hasNext()) {
      final Tuple2<Tuple2<String, String>, HoodieKey> entry = tuple2Iterator.next();
      final String colStatKeyHash = entry._1._1;
      final String fileName = entry._1._2;
      final HoodieKey hoodieKey = entry._2;

      columnKeyHashToHoodieKeysMap.computeIfAbsent(colStatKeyHash, e -> new ArrayList<>()).add(hoodieKey);
      columnKeyHashToFileNameMap.putIfAbsent(colStatKeyHash, fileName);
    }

    Map<String, HoodieColumnStats> columnKeyHashToStatMap =
        hoodieTable.getMetadataTable().getColumnStats(
            columnKeyHashToHoodieKeysMap.keySet().stream().collect(Collectors.toList()));

    for (Map.Entry<String, String> entry : columnKeyHashToFileNameMap.entrySet()) {
      ValidationUtils.checkState(columnKeyHashToHoodieKeysMap.containsKey(entry.getKey()));
      ValidationUtils.checkState(columnKeyHashToStatMap.containsKey(entry.getKey()));

      final String fileName = entry.getValue();
      final List<HoodieKey> keyList = columnKeyHashToHoodieKeysMap.get(entry.getKey());
      final HoodieColumnStats keyColumnStat = columnKeyHashToStatMap.get(entry.getKey());

      keyList.forEach(hoodieKey -> {
        if ((Objects.requireNonNull(keyColumnStat.getRangeLow()).compareTo(hoodieKey.getRecordKey()) <= 0)
            && (Objects.requireNonNull(keyColumnStat.getRangeHigh()).compareTo(hoodieKey.getRecordKey()) >= 0)) {
          result.add(new Tuple2<>(new Tuple2<>(FSUtils.getFileId(fileName), fileName), hoodieKey));
        }
      });
    }
    return result.iterator();
  }
}
