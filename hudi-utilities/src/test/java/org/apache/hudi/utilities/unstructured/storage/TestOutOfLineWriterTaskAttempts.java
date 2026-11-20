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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The write token of an out-of-line file comes from the Spark task attempt that writes it: a retried task writes under a new
 * token. Runs with out-of-line markers off, so a writer needs no timeline server, and the failed attempt's file stays orphaned.
 */
@Tag("functional")
class TestOutOfLineWriterTaskAttempts {
  private static final String INSTANT = "20261027120000000";

  @TempDir
  Path temp;

  @Test
  void retriedTaskWritesUnderANewWriteToken() throws Exception {
    String base = temp.resolve("table").toUri().toString();
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(layoutProperties())
        .setTableName("blob_table").setTableType(HoodieTableType.COPY_ON_WRITE).initTable(conf, base);
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    OutOfLineWriter.Factory writers = OutOfLineWriter.forTable(conf, base, props);

    // local[1,2]: one thread, and a failed task is attempted twice
    SparkConf sparkConf = new SparkConf().setMaster("local[1,2]").setAppName(getClass().getSimpleName())
        .set("spark.ui.enabled", "false").set("spark.driver.host", "127.0.0.1");
    List<String> committed;
    try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
      committed = jsc.parallelize(Collections.singletonList(1), 1).map(ignored -> {
        TaskContext task = TaskContext.get();
        HoodieStorage storage = HoodieStorageUtils.getStorage(base, conf.newInstance());
        String relative = writers.forInstant(storage, INSTANT).write("p", out -> out.write(task.attemptNumber()));
        if (task.attemptNumber() == 0) {
          throw new IllegalStateException("the first attempt fails after its write");
        }
        return relative + "|" + FSUtils.makeWriteToken(task.partitionId(), task.stageId(), task.taskAttemptId());
      }).collect();
    }

    assertEquals(1, committed.size(), "one successful attempt returned its path");
    String[] written = committed.get(0).split("\\|");
    String retriedName = written[0].substring(written[0].lastIndexOf('/') + 1);
    assertEquals(written[1], FSUtils.getWriteTokenFromBaseFile(retriedName), "the token of the attempt that wrote it");

    Set<String> names;
    try (Stream<Path> files = Files.list(temp.resolve("table").resolve(".hoodie/.blob/p"))) {
      // the local file system's .crc side files are not out-of-line files
      names = files.map(path -> path.getFileName().toString()).filter(name -> !name.startsWith("."))
          .collect(Collectors.toCollection(TreeSet::new));
    }
    assertEquals(2, names.size(), "the failed attempt's file is left orphaned: " + names);
    assertTrue(names.contains(retriedName), names.toString());
    Set<String> tokens = names.stream().map(FSUtils::getWriteTokenFromBaseFile).collect(Collectors.toSet());
    Set<String> fileIds = names.stream().map(FSUtils::getFileId).collect(Collectors.toSet());
    assertEquals(2, tokens.size(), "two attempts, two write tokens: " + tokens);
    assertEquals(2, fileIds.size(), "a fresh file id per file: " + fileIds);
    for (String name : names) {
      assertEquals(INSTANT, FSUtils.getCommitTime(name), name);
      assertTrue(name.endsWith(".bin"), name);
    }
    String[] first = tokens.stream().filter(token -> !token.equals(written[1])).findFirst()
        .orElseThrow(() -> new AssertionError("no token of the failed attempt: " + tokens)).split("-");
    String[] retried = written[1].split("-");
    assertEquals(first[0] + "-" + first[1], retried[0] + "-" + retried[1], "same task partition and stage");
    assertNotEquals(first[2], retried[2], "a different task attempt");
    assertFalse(Files.exists(temp.resolve("table").resolve(".hoodie/.temp/" + INSTANT)), "no marker folder for the instant");
  }

  private static Properties layoutProperties() {
    Properties props = new Properties();
    props.setProperty("hoodie.table.blob.layout.version", "1");
    return props;
  }

}
