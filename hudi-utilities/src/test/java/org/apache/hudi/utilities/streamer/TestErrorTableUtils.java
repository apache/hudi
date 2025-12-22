/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamerSchemaEvolutionBase.TestErrorTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ErrorTableUtils}.
 */
public class TestErrorTableUtils {
  @Test
  public void testGetErrorTableWriter() {
    SparkSession sparkSession = Mockito.mock(SparkSession.class);
    HoodieSparkEngineContext sparkContext = Mockito.mock(HoodieSparkEngineContext.class);
    FileSystem fileSystem = Mockito.mock(FileSystem.class);

    TypedProperties props = new TypedProperties();
    // No error table writer config
    assertThrows(IllegalArgumentException.class,
        () -> ErrorTableUtils.getErrorTableWriter(
            new HoodieStreamer.Config(), sparkSession, props, sparkContext, fileSystem));

    // Empty error table writer config
    props.put("hoodie.errortable.write.class", StringUtils.EMPTY_STRING);
    assertThrows(IllegalStateException.class,
        () -> ErrorTableUtils.getErrorTableWriter(
            new HoodieStreamer.Config(), sparkSession, props, sparkContext, fileSystem));

    // Proper error table writer config
    props.put("hoodie.errortable.write.class", TestErrorTable.class.getName());
    assertTrue(ErrorTableUtils.getErrorTableWriter(
        new HoodieStreamer.Config(), sparkSession, props, sparkContext, fileSystem).get() instanceof TestErrorTable);

    // Wrong error table writer config
    props.put("hoodie.errortable.write.class", HoodieConfig.class.getName());
    assertThrows(HoodieException.class,
        () -> ErrorTableUtils.getErrorTableWriter(
            new HoodieStreamer.Config(), sparkSession, props, sparkContext, fileSystem));
  }
}
