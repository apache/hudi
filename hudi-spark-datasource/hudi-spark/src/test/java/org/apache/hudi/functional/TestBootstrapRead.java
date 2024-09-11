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

package org.apache.hudi.functional;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

/**
 * Tests different layouts for bootstrap base path
 */
@Tag("functional")
public class TestBootstrapRead extends TestBootstrapReadBase {
  private static Stream<Arguments> testArgs() {
    boolean fullTest = false;
    Stream.Builder<Arguments> b = Stream.builder();
    if (fullTest) {
      String[] bootstrapType = {"full", "metadata", "mixed"};
      Boolean[] dashPartitions = {true,false};
      HoodieTableType[] tableType = {COPY_ON_WRITE, MERGE_ON_READ};
      Integer[] nPartitions = {0, 1, 2};
      for (HoodieTableType tt : tableType) {
        for (Boolean dash : dashPartitions) {
          for (String bt : bootstrapType) {
            for (Integer n : nPartitions) {
              // can't be mixed bootstrap if it's nonpartitioned
              // don't need to test slash partitions if it's nonpartitioned
              if ((!bt.equals("mixed") && dash) || n > 0) {
                b.add(Arguments.of(bt, dash, tt, n));
              }
            }
          }
        }
      }
    } else {
      b.add(Arguments.of("metadata", true, COPY_ON_WRITE, 0));
      b.add(Arguments.of("mixed", false, MERGE_ON_READ, 2));
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testBootstrapFunctional(String bootstrapType, Boolean dashPartitions, HoodieTableType tableType, Integer nPartitions) {
    this.bootstrapType = bootstrapType;
    this.dashPartitions = dashPartitions;
    this.tableType = tableType;
    this.nPartitions = nPartitions;
    setupDirs();

    // do bootstrap
    Map<String, String> options = setBootstrapOptions();
    Dataset<Row> bootstrapDf = sparkSession.emptyDataFrame();
    bootstrapDf.write().format("hudi")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save(bootstrapTargetPath);
    compareTables();
    verifyMetaColOnlyRead(0);

    // do upserts
    options = basicOptions();
    doUpdate(options, "001");
    compareTables();
    verifyMetaColOnlyRead(1);

    doInsert(options, "002");
    compareTables();
    verifyMetaColOnlyRead(2);
  }
}
