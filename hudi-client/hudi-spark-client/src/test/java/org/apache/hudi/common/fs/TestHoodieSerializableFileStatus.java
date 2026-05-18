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

package org.apache.hudi.common.fs;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.hadoop.fs.HoodieSerializableFileStatus;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test the if {@link HoodieSerializableFileStatus} is serializable
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestHoodieSerializableFileStatus extends HoodieSparkClientTestHarness {

  HoodieEngineContext engineContext;
  List<Path> testPaths;

  @BeforeAll
  public void setUp() throws IOException {
    initSparkContexts();
    testPaths = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      testPaths.add(new Path("s3://table-bucket/"));
    }
    engineContext = new HoodieSparkEngineContext(jsc);
  }

  @AfterAll
  public void tearDown() {
    cleanupSparkContexts();
  }

  @Test
  public void testNonSerializableFileStatus() {
    Exception e = Assertions.assertThrows(SparkException.class,
        () -> {
          List<FileStatus> statuses = engineContext.flatMap(testPaths, path -> {
            FileSystem fileSystem = new NonSerializableFileSystem();
            return Arrays.stream(fileSystem.listStatus(path));
          }, 5);
        },
        "Serialization is supposed to fail!");
    Assertions.assertTrue(e.getMessage().contains("com.esotericsoftware.kryo.KryoException: java.lang.IllegalArgumentException: Unable to create serializer"));
  }

  @Test
  public void testHoodieFileStatusSerialization() {
    Assertions.assertDoesNotThrow(() -> engineContext.flatMap(testPaths, path -> {
      FileSystem fileSystem = new NonSerializableFileSystem();
      return Arrays.stream(HoodieSerializableFileStatus.fromFileStatuses(fileSystem.listStatus(path)));
    }, 5));
  }
}
