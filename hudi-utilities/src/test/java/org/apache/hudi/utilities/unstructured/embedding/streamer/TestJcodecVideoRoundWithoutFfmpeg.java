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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A JCodec video round in a JVM WITHOUT FFmpeg on its class path: the FFmpeg jars (org.bytedeco) are on the test class path, so
 * {@link JcodecVideoRoundScenarios} runs in a child JVM whose class path is this one's without them ({@link ScenarioJvm}). The
 * child's outcome of every scenario test is asserted here, its failures and the tail of its log in the message.
 */
@Tag("functional")
class TestJcodecVideoRoundWithoutFfmpeg {

  /** The JCodec video round on both table types and in both modes: 4 cases. */
  @Test
  void jcodecVideoRoundOnBothTableTypesAndModes(@TempDir Path workDir) throws Exception {
    assertAllSucceed(ScenarioJvm.run(workDir, JcodecVideoRoundScenarios.class.getName(), JcodecVideoRoundScenarios.JCODEC_ROUND), 4);
  }

  /** The harness itself: a round without video in the child JVM, which also checks FFmpeg is absent there. */
  @Test
  void roundWithoutVideoRunsInTheChildJvm(@TempDir Path workDir) throws Exception {
    assertAllSucceed(ScenarioJvm.run(workDir, JcodecVideoRoundScenarios.class.getName(), JcodecVideoRoundScenarios.SANITY), 1);
  }

  private static void assertAllSucceed(ScenarioJvm.Result result, int tests) {
    List<String> succeeded = result.withStatus("SUCCESSFUL");
    assertEquals(0, result.exitCode, result.describe());
    assertTrue(result.withStatus("FAILED").isEmpty() && result.withStatus("ABORTED").isEmpty(), result.describe());
    assertEquals(tests, succeeded.size(), result.describe());
  }
}
