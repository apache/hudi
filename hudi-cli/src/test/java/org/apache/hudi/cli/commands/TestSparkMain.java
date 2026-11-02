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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.util.Option;

import com.beust.jcommander.ParameterException;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

class TestSparkMain {

  @ParameterizedTest
  @EnumSource(value = SparkCommand.class, names = {"IMPORT", "UPSERT"}, mode = EnumSource.Mode.EXCLUDE)
  void testEveryCommandRoundTripsInPositionalOrder(SparkCommand command) {
    String[] names = expectedParamNames(command);
    List<String> pairs = new ArrayList<>();
    // Deliberately pass the named arguments in reverse order.
    for (int i = names.length - 1; i >= 0; i--) {
      pairs.add(names[i]);
      pairs.add("value-for-" + names[i]);
    }
    String[] actual = launchArgs(command, pairs.toArray(new String[0]));
    List<String> expected = new ArrayList<>(Arrays.asList(command.name(), "local", "1G"));
    for (String name : names) {
      expected.add("value-for-" + name);
    }
    assertArrayEquals(expected.toArray(new String[0]), actual);
    command.assertEq(actual.length);
  }

  @ParameterizedTest
  @ValueSource(strings = {" region=us ", "\tregion=us\t", "\"region=us\"", "@region=us", "region=x=y", "", "region=你好"})
  void testPartitionValuesArePreserved(String value) {
    assertArrayEquals(new String[] {"RENAME_PARTITION", "local", "1G", " /table ", value, "new "},
        launchArgs(SparkCommand.RENAME_PARTITION, "basePath", " /table ", "oldPartition", value, "newPartition", "new "));
    String[] savepoint = launchArgs(SparkCommand.SAVEPOINT,
        "commitTime", "001", "user", " user ", "comments", value, "basePath", "/table");
    assertEquals(value, savepoint[5]);
    assertEquals(" user ", savepoint[4]);
  }

  @Test
  void testTrailingConfigsAndPropsPathArePreserved() {
    String[] configs = {"hoodie.a= 1 ", "spark.b=x=y", "hoodie.a=2", "\"hoodie.quoted=x\"", "hoodie.list=a,b", "\thoodie.c=3\t"};
    RecordingLauncher launcher = new RecordingLauncher();
    SparkMain.addNamedAppArgs(launcher, SparkCommand.CLEAN, "local", "1G", "basePath", "/table", "propsFilePath", " /props ");
    launcher.addAppArgs(configs);
    String[] positional = SparkMain.namedArgsToPositional(launcher.argv());
    assertEquals(" /props ", SparkCommand.CLEAN.getPropsFilePath(positional));
    assertEquals(Arrays.asList(configs), SparkCommand.CLEAN.makeConfigs(positional));
  }

  @Test
  void testOmittedAndNullPropsPathKeepTheirSlot() {
    for (String[] pairs : new String[][] {{"basePath", "/table"}, {"basePath", "/table", "propsFilePath", null}}) {
      String[] positional = launchArgs(SparkCommand.CLEAN, pairs);
      assertArrayEquals(new String[] {"CLEAN", "local", "1G", "/table", ""}, positional);
      assertNull(SparkCommand.CLEAN.getPropsFilePath(positional));
    }
    String[] positional = launchArgs(SparkCommand.SAVEPOINT,
        "commitTime", "001", "user", "user", "comments", null, "basePath", "/table");
    assertEquals("", positional[5]);
  }

  @Test
  void testLauncherRejectsInvalidPairsBeforeAddingArgs() {
    for (String[] pairs : new String[][] {
        {"basePath"}, {}, {"basePth", "/table"}, {"basePath", "/table", "basePth", "/other"},
        {"basePath", "/table", "basePath", "/other"}}) {
      RecordingLauncher launcher = new RecordingLauncher();
      assertThrows(IllegalArgumentException.class,
          () -> SparkMain.addNamedAppArgs(launcher, SparkCommand.CLEAN, "local", "1G", pairs));
      assertTrue(launcher.args.isEmpty());
    }
  }

  @Test
  void testDirectNamedInvocationRejectsMissingAndMisspelledKeys() {
    for (String[] params : new String[][] {{}, {"-DbasePth=/table"}, {"-DbasePath=/table", "-DbasePth=/other"},
        {"-DbasePath=/table", "-Dhoodie.a=1"}}) {
      List<String> argv = new ArrayList<>(Arrays.asList("--command", "CLEAN", "--master", "local", "--memory", "1G"));
      argv.addAll(Arrays.asList(params));
      assertThrows(IllegalArgumentException.class, () -> SparkMain.namedArgsToPositional(argv.toArray(new String[0])));
      // Invalid named input must fail in main before a Spark context is created.
      assertThrows(IllegalArgumentException.class, () -> SparkMain.main(argv.toArray(new String[0])));
    }
  }

  @Test
  void testAliasesSplitDynamicOptionsAndConfigSeparator() {
    String[] positional = SparkMain.namedArgsToPositional(new String[] {
        "-command", "CLEAN", "-master", "local", "-memory", "1G", "-D", "basePath= /table ", "--", "hoodie.a=1 "});
    assertArrayEquals(new String[] {"CLEAN", "local", "1G", " /table ", "", "hoodie.a=1 "}, positional);
  }

  @Test
  void testRequiredOptionsAndUnknownOptionsAreValidated() {
    assertThrows(ParameterException.class, () -> SparkMain.namedArgsToPositional(new String[] {
        "--command", "CLEAN", "--master", "local", "-DbasePath=/table"}));
    assertThrows(ParameterException.class, () -> SparkMain.namedArgsToPositional(new String[] {
        "--command", "CLEAN", "--master", "local", "--memory", "1G", "--typo", "-DbasePath=/table"}));
    assertThrows(IllegalArgumentException.class, () -> SparkMain.namedArgsToPositional(new String[] {"--command"}));
  }

  @ParameterizedTest
  @EnumSource(value = SparkCommand.class, names = {"IMPORT", "UPSERT"})
  void testUnsupportedCommandsRejectNamedArguments(SparkCommand command) {
    assertThrows(IllegalArgumentException.class, () -> launchArgs(command));
    assertThrows(IllegalArgumentException.class, () -> SparkMain.namedArgsToPositional(new String[] {
        "--command", command.name(), "--master", "local", "--memory", "1G"}));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMainAcceptsLegacyAndNamedInvocations(boolean named) {
    String[] argv;
    if (named) {
      RecordingLauncher launcher = new RecordingLauncher();
      SparkMain.addNamedAppArgs(launcher, SparkCommand.CLEAN, "local", "1G", "basePath", "/table");
      argv = launcher.argv();
    } else {
      argv = new String[] {"CLEAN", "local", "1G", "/table", ""};
    }
    // Stop at context creation so this tests main's argv handling without starting Spark or exiting the JVM.
    RuntimeException stop = new RuntimeException("Stop before Spark starts");
    try (MockedStatic<SparkUtil> sparkUtil = mockStatic(SparkUtil.class)) {
      sparkUtil.when(() -> SparkUtil.initJavaSparkContext("hoodie-cli-CLEAN", Option.of("local"), Option.of("1G")))
          .thenThrow(stop);
      assertSame(stop, assertThrows(RuntimeException.class, () -> SparkMain.main(argv)));
    }
  }

  private static String[] launchArgs(SparkCommand command, String... pairs) {
    RecordingLauncher launcher = new RecordingLauncher();
    SparkMain.addNamedAppArgs(launcher, command, "local", "1G", pairs);
    return SparkMain.namedArgsToPositional(launcher.argv());
  }

  private static class RecordingLauncher extends SparkLauncher {
    private final List<String> args = new ArrayList<>();

    @Override
    public SparkLauncher addAppArgs(String... values) {
      args.addAll(Arrays.asList(values));
      return this;
    }

    private String[] argv() {
      return args.toArray(new String[0]);
    }
  }

  // Independent fixtures document the positional contract used by main's dispatch branches.
  private static String[] expectedParamNames(SparkCommand command) {
    switch (command) {
      case BOOTSTRAP:
        return new String[] {"tableName", "tableType", "targetPath", "srcPath", "rowKeyField", "partitionPathField", "parallelism",
            "schemaProviderClass", "bootstrapIndexClass", "selectorClass", "keyGeneratorClass",
            "fullBootstrapInputProvider", "recordMergeMode", "payloadClass", "recordMergeStrategyId",
            "recordMergeImplClasses", "enableHiveSync", "propsFilePath"};
      case ROLLBACK:
        return new String[] {"instantTime", "basePath", "rollbackUsingMarkers"};
      case DEDUPLICATE:
        return new String[] {"duplicatedPartitionPath", "repairedOutputPath", "basePath", "dryRun", "dedupeType"};
      case ROLLBACK_TO_SAVEPOINT:
        return new String[] {"savepointTime", "basePath", "lazyCleanPolicy"};
      case SAVEPOINT:
        return new String[] {"commitTime", "user", "comments", "basePath"};
      case COMPACT_SCHEDULE:
        return new String[] {"basePath", "tableName", "propsFilePath"};
      case COMPACT_RUN:
        return new String[] {"basePath", "tableName", "compactionInstant", "parallelism", "schemaPath", "retry", "propsFilePath"};
      case COMPACT_SCHEDULE_AND_EXECUTE:
        return new String[] {"basePath", "tableName", "parallelism", "schemaPath", "retry", "propsFilePath"};
      case COMPACT_UNSCHEDULE_PLAN:
        return new String[] {"basePath", "compactionInstant", "outputPath", "parallelism", "skipValidation", "dryRun"};
      case COMPACT_UNSCHEDULE_FILE:
        return new String[] {"basePath", "fileId", "partitionPath", "outputPath", "parallelism", "skipValidation", "dryRun"};
      case COMPACT_VALIDATE:
        return new String[] {"basePath", "compactionInstant", "outputPath", "parallelism"};
      case COMPACT_REPAIR:
        return new String[] {"basePath", "compactionInstant", "outputPath", "parallelism", "dryRun"};
      case CLUSTERING_SCHEDULE:
        return new String[] {"basePath", "tableName", "propsFilePath"};
      case CLUSTERING_RUN:
        return new String[] {"basePath", "tableName", "clusteringInstant", "parallelism", "retry", "propsFilePath"};
      case CLUSTERING_SCHEDULE_AND_EXECUTE:
        return new String[] {"basePath", "tableName", "parallelism", "retry", "propsFilePath"};
      case CLEAN:
        return new String[] {"basePath", "propsFilePath"};
      case DELETE_MARKER:
        return new String[] {"instantTime", "basePath"};
      case DELETE_SAVEPOINT:
        return new String[] {"savepointTime", "basePath"};
      case UPGRADE:
        return new String[] {"basePath", "toVersionName"};
      case DOWNGRADE:
        return new String[] {"basePath", "toVersionName"};
      case REPAIR_DEPRECATED_PARTITION:
        return new String[] {"basePath"};
      case RENAME_PARTITION:
        return new String[] {"basePath", "oldPartition", "newPartition"};
      case ARCHIVE:
        return new String[] {"minCommits", "maxCommits", "commitsRetained", "enableMetadata", "basePath"};
      default:
        throw new IllegalArgumentException("Unsupported command: " + command);
    }
  }
}
