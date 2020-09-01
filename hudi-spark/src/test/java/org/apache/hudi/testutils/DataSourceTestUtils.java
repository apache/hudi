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

import org.apache.hudi.common.util.FileIOUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;

/**
 * Test utils for data source tests.
 */
public class DataSourceTestUtils {

  public static Schema getStructTypeExampleSchema() throws IOException {
    return new Schema.Parser().parse(FileIOUtils.readAsUTFString(DataSourceTestUtils.class.getResourceAsStream("/exampleSchema.txt")));
  }

  public static List<Row> generateRandomRows(int count) {
    Random random = new Random();
    List<Row> toReturn = new ArrayList<>();
    List<String> partitions = Arrays.asList(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH});
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[3];
      values[0] = UUID.randomUUID().toString();
      values[1] = partitions.get(random.nextInt(3));
      values[2] = new Date().getTime();
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }
}
