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

package org.apache.hudi.cli;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.spark.SparkConf;

import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestSparkUtil {

  @Test
  public void testInitSparkLauncher() throws URISyntaxException {
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(null);
    assertNotNull(sparkLauncher);
  }

  @Test
  public void testGetDefaultSparkConf() {
    SparkConf sparkConf = SparkUtil.getDefaultConf("test-spark-app", Option.of(""));
    assertEquals(SparkUtil.DEFAULT_SPARK_MASTER, sparkConf.get("spark.master"));
  }
}
