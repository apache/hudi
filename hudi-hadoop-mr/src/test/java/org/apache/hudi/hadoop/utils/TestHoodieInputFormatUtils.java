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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.hadoop.HoodieHFileInputFormat;
import org.apache.hudi.hadoop.HoodieLanceInputFormat;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.HoodieVortexInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieLanceRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieVortexRealtimeInputFormat;

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The input format, output format and SerDe names are returned as string literals so that a catalog
 * sync never class-loads them (see the comment on the constants in {@link HoodieInputFormatUtils}).
 * That trades compile-time safety for a runtime one, so these assertions put it back: Hive is on the
 * test classpath here, and each constant is checked against the class it names. A rename that would
 * otherwise ship a dangling name to a metastore fails here instead.
 */
class TestHoodieInputFormatUtils {

  @Test
  void testInputFormatConstantsMatchTheirClasses() {
    assertEquals(HoodieParquetInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_PARQUET_INPUT_FORMAT);
    assertEquals(HoodieParquetRealtimeInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_PARQUET_REALTIME_INPUT_FORMAT);
    assertEquals(HoodieHFileInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_HFILE_INPUT_FORMAT);
    assertEquals(HoodieHFileRealtimeInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_HFILE_REALTIME_INPUT_FORMAT);
    assertEquals(HoodieLanceInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_LANCE_INPUT_FORMAT);
    assertEquals(HoodieLanceRealtimeInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_LANCE_REALTIME_INPUT_FORMAT);
    assertEquals(HoodieVortexInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_VORTEX_INPUT_FORMAT);
    assertEquals(HoodieVortexRealtimeInputFormat.class.getName(), HoodieInputFormatUtils.HOODIE_VORTEX_REALTIME_INPUT_FORMAT);
    assertEquals(OrcInputFormat.class.getName(), HoodieInputFormatUtils.ORC_INPUT_FORMAT);
  }

  @Test
  void testOutputFormatAndSerDeConstantsMatchTheirClasses() {
    assertEquals(MapredParquetOutputFormat.class.getName(), HoodieInputFormatUtils.MAPRED_PARQUET_OUTPUT_FORMAT);
    assertEquals(OrcOutputFormat.class.getName(), HoodieInputFormatUtils.ORC_OUTPUT_FORMAT);
    assertEquals(ParquetHiveSerDe.class.getName(), HoodieInputFormatUtils.PARQUET_HIVE_SERDE);
    assertEquals(OrcSerde.class.getName(), HoodieInputFormatUtils.ORC_SERDE);
  }

  /** The names the catalog syncs actually ask for, so the switch wiring is covered too. */
  @Test
  void testClassNamesForParquetAndOrc() {
    assertEquals(HoodieParquetInputFormat.class.getName(),
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, false));
    assertEquals(HoodieParquetRealtimeInputFormat.class.getName(),
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, true));
    assertEquals(MapredParquetOutputFormat.class.getName(),
        HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET));
    assertEquals(ParquetHiveSerDe.class.getName(),
        HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET));
    assertEquals(OrcInputFormat.class.getName(),
        HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.ORC, false));
    assertEquals(OrcOutputFormat.class.getName(),
        HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.ORC));
    assertEquals(OrcSerde.class.getName(),
        HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.ORC));
  }
}
