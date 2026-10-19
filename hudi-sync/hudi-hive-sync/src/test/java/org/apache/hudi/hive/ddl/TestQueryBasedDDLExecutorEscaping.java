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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DECODE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestQueryBasedDDLExecutorEscaping {

  static Stream<Arguments> partitionValues() {
    return Stream.of(
        Arguments.of("plain", false, "plain"),
        Arguments.of("O'Reilly", false, "O'Reilly"),
        Arguments.of("back\\slash", false, "back\\slash"),
        Arguments.of("trailing\\", false, "trailing\\"),
        Arguments.of("back\\'quote", false, "back\\'quote"),
        Arguments.of("O%27Reilly%5Ctail", true, "O'Reilly\\tail"),
        Arguments.of("O%27Reilly%5Ctail", false, "O%27Reilly%5Ctail"),
        Arguments.of("x') LOCATION 'other' --", false, "x') LOCATION 'other' --"));
  }

  @ParameterizedTest
  @MethodSource("partitionValues")
  void partitionDdlPreservesLiteralValues(String value, boolean decode, String expectedValue) throws Exception {
    // Quotes and backslashes in the base path must also survive Hive's literal decoding.
    String basePath = "/tmp/warehouse's\\data";
    TypedProperties props = new TypedProperties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "db");
    props.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "p");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), MultiPartKeysValueExtractor.class.getName());
    props.setProperty(META_SYNC_DECODE_PARTITION.key(), Boolean.toString(decode));
    RecordingExecutor executor = new RecordingExecutor(new HiveSyncConfig(props));
    String partition = "p=" + value;
    List<String> partitions = Collections.singletonList(partition);
    // Decoding the partition value must not decode the physical directory location.
    List<String> expectedWithLocation = Arrays.asList(expectedValue, basePath + "/" + partition);

    executor.addPartitionsToTable("tbl", partitions);
    assertEquals(1, executor.executed.size());
    assertEquals(expectedWithLocation, parseLiterals(executor.executed.get(0)));

    executor.executed.clear();
    executor.updatePartitionsToTable("tbl", partitions);
    assertEquals(2, executor.executed.size());
    // Hive's SET LOCATION AST visits the location before the partition specification.
    assertEquals(Arrays.asList(basePath + "/" + partition, expectedValue), parseLiterals(executor.executed.get(1)));

    executor.executed.clear();
    executor.touchPartitionsToTable("tbl", partitions);
    assertEquals(2, executor.executed.size());
    assertEquals(Collections.singletonList(expectedValue), parseLiterals(executor.executed.get(1)));
  }

  private static List<String> parseLiterals(String sql) throws Exception {
    List<String> literals = new ArrayList<>();
    collectLiterals(new ParseDriver().parse(sql), literals);
    return literals;
  }

  private static void collectLiterals(ASTNode node, List<String> literals) {
    if (node.getType() == HiveParser.StringLiteral) {
      literals.add(BaseSemanticAnalyzer.unescapeSQLString(node.getText()));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collectLiterals((ASTNode) node.getChild(i), literals);
    }
  }

  private static class RecordingExecutor extends QueryBasedDDLExecutor {
    private final List<String> executed = new ArrayList<>();

    RecordingExecutor(HiveSyncConfig config) {
      super(config);
    }

    @Override
    public void runSQL(String sql) {
      executed.add(sql);
    }

    @Override
    public Map<String, String> getTableSchema(String tableName) {
      return Collections.emptyMap();
    }

    @Override
    public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
      // Not used by these tests.
    }

    @Override
    public void close() {
      // No resources to close.
    }
  }
}
