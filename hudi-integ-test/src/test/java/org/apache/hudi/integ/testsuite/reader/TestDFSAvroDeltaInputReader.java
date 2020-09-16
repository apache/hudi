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

package org.apache.hudi.integ.testsuite.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.utils.TestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link DFSAvroDeltaInputReader} by issuing analyzeSingleFile and read from it.
 */
public class TestDFSAvroDeltaInputReader extends UtilitiesTestBase {

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  @Disabled
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
