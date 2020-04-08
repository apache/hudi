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

import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test utils for data source tests.
 */
public class DataSourceTestUtils {

  public static Option<String> convertToString(HoodieRecord record) {
    try {
      String str = ((TestRawTripPayload) record.getData()).getJsonData();
      str = "{" + str.substring(str.indexOf("\"timestamp\":"));
      // Remove the last } bracket
      str = str.substring(0, str.length() - 1);
      return Option.of(str + ", \"partition\": \"" + record.getPartitionPath() + "\"}");
    } catch (IOException e) {
      return Option.empty();
    }
  }

  public static List<String> convertToStringList(List<HoodieRecord> records) {
    return records.stream().map(DataSourceTestUtils::convertToString).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
  }

  public static List<String> convertKeysToStringList(List<HoodieKey> keys) {
    return keys.stream()
        .map(hr -> "{\"_row_key\":\"" + hr.getRecordKey() + "\",\"partition\":\"" + hr.getPartitionPath() + "\"}")
        .collect(Collectors.toList());
  }

  public static class NoOpBulkInsertPartitioner<T extends HoodieRecordPayload>
          implements UserDefinedBulkInsertPartitioner<T> {

    @Override
    public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
      return records;
    }
  }

}
