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

package org.apache.hudi.bench.reader;

import org.apache.hudi.bench.utils.TestUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilitiesTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class TestDFSAvroDeltaInputReader extends UtilitiesTestBase {

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  public void testDFSSinkReader() throws IOException {
    FileSystem fs = FSUtils.getFs(dfsBasePath, new Configuration());
    // Create 10 avro files with 10 records each
    TestUtils.createAvroFiles(jsc, sparkSession, dfsBasePath, 10, 10);
    FileStatus[] statuses = fs.globStatus(new Path(dfsBasePath + "/*/*.avro"));
    DFSAvroDeltaInputReader reader =
        new DFSAvroDeltaInputReader(sparkSession, TestUtils.getSchema().toString(), dfsBasePath, Option.empty(),
            Option.empty());
    assertEquals(reader.analyzeSingleFile(statuses[0].getPath().toString()), 5);
    assertEquals(reader.read(100).count(), 100);
    assertEquals(reader.read(1000).count(), 100);
    assertEquals(reader.read(10).count(), 10);
    assertTrue(reader.read(11).count() > 11);
  }

}
