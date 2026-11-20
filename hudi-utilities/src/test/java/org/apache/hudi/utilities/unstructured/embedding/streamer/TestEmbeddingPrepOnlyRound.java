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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The embedding job's {@code prep_only} round commits every prepared row in one commit: the round has no model and must not look
 * one up. One streamer round is driven directly here, the way a rendered job runs it.
 */
@Tag("functional")
class TestEmbeddingPrepOnlyRound extends EmbeddingRoundTestBase {

  @Test
  void prepOnlyRoundCommitsThePreparedRowsWithoutAModel() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    announce(a);
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_ONLY);
    HoodieStreamer.Config cfg = EmbeddingStreamers.config(table());
    // the job names its table by --target-base-path alone, as a rendered job does
    TypedProperties job = TypedProperties.copy(props);
    job.remove(HoodieWriteConfig.BASE_PATH.key());
    HoodieStreamer streamer = new HoodieStreamer(cfg, JavaSparkContext.fromSparkContext(spark().sparkContext()), Option.of(job));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
      spark().catalog().clearCache();
    }
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size(), "one commit holds the prepared rows");
    assertEquals(2, rows(table()).size(), "one row per chunk of the text object");
  }

  /** The embedding source runs its transformer when the job names no transformer: the same round commits the same rows. */
  @Test
  void embeddingTransformerRunsWhenTheJobNamesNone() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    announce(a);
    HoodieStreamer.Config cfg = EmbeddingStreamers.config(table());
    cfg.transformerClassNames = null;
    TypedProperties job = TypedProperties.copy(props(table(), EmbeddingTableConfig.MODE_PREP_ONLY));
    job.remove(HoodieWriteConfig.BASE_PATH.key());
    HoodieStreamer streamer = new HoodieStreamer(cfg, JavaSparkContext.fromSparkContext(spark().sparkContext()), Option.of(job));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
      spark().catalog().clearCache();
    }
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size(), "one commit holds the prepared rows");
    assertEquals(2, rows(table()).size(), "one row per chunk of the text object");
  }
}
