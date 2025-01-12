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

package org.apache.hudi.testutils;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import org.apache.avro.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.collectColumnRangeMetadata;

/**
 * Util methods used in tests to fetch col stats records for a log file.
 */
public class LogFileColStatsTestUtil {

  public static Option<Row> getLogFileColumnRangeMetadata(String filePath, HoodieTableMetaClient datasetMetaClient, String latestCommitTime,
                                                  List<String> columnsToIndex, Option<Schema> writerSchemaOpt,
                                                  int maxBufferSize) throws IOException {
    if (writerSchemaOpt.isPresent()) {
      List<Pair<String, Schema.Field>> fieldsToIndex = columnsToIndex.stream()
          .map(fieldName -> HoodieTableMetadataUtil.getSchemaForField(writerSchemaOpt.get(), fieldName, ""))
          .collect(Collectors.toList());
      List<HoodieRecord> records = new ArrayList<>();
      HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
          .withStorage(datasetMetaClient.getStorage())
          .withBasePath(datasetMetaClient.getBasePath())
          .withLogFilePaths(Collections.singletonList(filePath))
          .withBufferSize(maxBufferSize)
          .withLatestInstantTime(latestCommitTime)
          .withReaderSchema(writerSchemaOpt.get())
          .withLogRecordScannerCallback(records::add)
          .build();
      scanner.scan();
      if (records.isEmpty()) {
        return Option.empty();
      }
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
          collectColumnRangeMetadata(records, fieldsToIndex, filePath, writerSchemaOpt.get());
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = new ArrayList<>(columnRangeMetadataMap.values());
      return Option.of(getColStatsEntry(filePath, columnRangeMetadataList));
    } else {
      throw new HoodieException("Writer schema needs to be set");
    }
  }

  private static Row getColStatsEntry(String logFilePath, List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList) {
    Collections.sort(columnRangeMetadataList, (o1, o2) -> o1.getColumnName().compareTo(o2.getColumnName()));
    Object[] values = new Object[(columnRangeMetadataList.size() * 3) + 2];
    values[0] = logFilePath.substring(logFilePath.lastIndexOf("/") + 1);
    values[1] = columnRangeMetadataList.get(0).getValueCount();
    int counter = 2;
    for (HoodieColumnRangeMetadata columnRangeMetadata: columnRangeMetadataList) {
      values[counter++] = columnRangeMetadata.getValueCount();
      values[counter++] = columnRangeMetadata.getMinValue();
      values[counter++] = columnRangeMetadata.getMaxValue();
    }
    return new GenericRow(values);
  }

  public static Option<Schema> getSchemaForTable(HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    return Option.of(schemaResolver.getTableAvroSchema());
  }
}

