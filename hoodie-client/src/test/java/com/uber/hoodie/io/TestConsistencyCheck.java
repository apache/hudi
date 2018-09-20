/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.uber.hoodie.common.HoodieClientTestUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestConsistencyCheck {

  private String basePath;
  private JavaSparkContext jsc;

  @Before
  public void setup() throws IOException {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("ConsistencyCheckTest"));
    TemporaryFolder testFolder = new TemporaryFolder();
    testFolder.create();
    basePath = testFolder.getRoot().getAbsolutePath();
  }

  @After
  public void teardown() {
    if (jsc != null) {
      jsc.stop();
    }
    File testFolderPath = new File(basePath);
    if (testFolderPath.exists()) {
      testFolderPath.delete();
    }
  }

  @Test
  public void testExponentialBackoff() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    JavaSparkContext jscSpy = spy(jsc);

    ConsistencyCheck failing = new ConsistencyCheck(basePath,
        Arrays.asList("partition/path/f1_0_000.parquet", "partition/path/f2_0_000.parquet"),
        jscSpy, 2);
    long startMs = System.currentTimeMillis();
    assertEquals(1, failing.check(5, 10).size());
    assertTrue((System.currentTimeMillis() - startMs) > (10 + 20 + 40 + 80));
    verify(jscSpy, times(5)).parallelize(anyList(), anyInt());
  }

  @Test
  public void testCheckPassingAndFailing() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f2");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f3");

    ConsistencyCheck passing = new ConsistencyCheck(basePath,
        Arrays.asList("partition/path/f1_0_000.parquet", "partition/path/f2_0_000.parquet"),
        jsc, 2);
    assertEquals(0, passing.check(1, 1000).size());

    ConsistencyCheck failing = new ConsistencyCheck(basePath,
        Arrays.asList("partition/path/f1_0_000.parquet", "partition/path/f4_0_000.parquet"),
        jsc, 2);
    assertEquals(1, failing.check(1, 1000).size());
  }
}
