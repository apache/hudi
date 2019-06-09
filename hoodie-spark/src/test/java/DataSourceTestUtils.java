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

import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Test utils for data source tests.
 */
public class DataSourceTestUtils {

  public static Optional<String> convertToString(HoodieRecord record) {
    try {
      String str = ((TestRawTripPayload) record.getData()).getJsonData();
      str = "{" + str.substring(str.indexOf("\"timestamp\":"));
      return Optional.of(str.replaceAll("}",
          ", \"partition\": \"" + record.getPartitionPath() + "\"}"));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  public static List<String> convertToStringList(List<HoodieRecord> records) {
    return records.stream().map(hr -> convertToString(hr)).filter(os -> os.isPresent())
        .map(os -> os.get()).collect(Collectors.toList());
  }
}
