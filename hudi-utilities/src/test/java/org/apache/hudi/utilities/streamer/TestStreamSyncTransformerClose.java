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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Retires a successful round's cache when StreamSync is closed for a rebuild in the same SparkContext. */
@Tag("functional")
public class TestStreamSyncTransformerClose extends UtilitiesTestBase {
  @BeforeAll
  static void startServices() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void stopServices() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Test
  void stockTransformerWithoutCloseWorksThroughChainAndStreamerShutdown() {
    TypedProperties props = new TypedProperties();
    Transformer stock = (context, session, rows, properties) -> rows;
    Transformer chain = new ChainedTransformer(Collections.singletonList(stock));
    Dataset<Row> input = sparkSession.range(3).toDF();
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    StreamSync sync = new StreamSync(cfg, sparkSession, props, new HoodieSparkEngineContext(jsc), null, null,
        client -> true, null, Option.empty(), null, Option.of(chain), false);
    assertEquals(input.collectAsList(), chain.apply(jsc, sparkSession, input, props).collectAsList());
    sync.close();
    sync.close();
    assertEquals(input.collectAsList(), chain.apply(jsc, sparkSession, input, props).collectAsList());
  }

  @Test
  void repeatedStreamerCloseReleasesTheLastQuarantineOutput() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", BlobTableSchema.SOURCE_PATH_SHA);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    Row row = RowFactory.create("sha", 1234L, "file:///input.pdf", "input", "pdf", 1L, "application/pdf",
        RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {9}, null));
    Dataset<Row> input = sparkSession.createDataFrame(Collections.singletonList(row), BlobTableSchema.schema());
    int before = jsc.getPersistentRDDs().size();
    for (int rebuild = 0; rebuild < 3; rebuild++) {
      Transformer transformer = new ChainedTransformer(Collections.singletonList(new BlobQuarantineTransformer()));
      HoodieStreamer.Config cfg = new HoodieStreamer.Config();
      StreamSync sync = new StreamSync(cfg, sparkSession, props, new HoodieSparkEngineContext(jsc), null, null,
          client -> true, null, Option.empty(), null, Option.of(transformer), false);
      Dataset<Row> output = transformer.apply(jsc, sparkSession, input, props);
      try {
        assertEquals(1, output.count());
        assertTrue(output.storageLevel().useMemory() || output.storageLevel().useDisk());
        assertEquals(before + 1, jsc.getPersistentRDDs().size(), "the live round owns exactly one persisted RDD");
        sync.close();
        assertEquals(StorageLevel.NONE(), output.storageLevel());
        assertEquals(before, jsc.getPersistentRDDs().size(), "a rebuild must retire the previous streamer's cache");
        sync.close();
      } finally {
        output.unpersist(true);
      }
    }
  }
}
