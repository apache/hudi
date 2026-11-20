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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSchemaGuard;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transformer each job of {@link UnstructuredDataSource} binds: the blob job runs the quarantine transformer and refuses the embedding
 * transformer, the embedding job the reverse. Only the source's contract is built, from the mode: no source, no Spark.
 */
public class TestUnstructuredSourceTransformers {

  private static final String QUARANTINE = BlobQuarantineTransformer.class.getName();
  private static final String EMBEDDING = EmbeddingTableTransformer.class.getName();
  private static final String GUARD = UnstructuredSchemaGuard.class.getName();

  private static UnstructuredSourceContract source(UnstructuredMode mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode.configValue());
    return new UnstructuredSourceContract(props);
  }

  @Test
  void blobSourceRunsTheQuarantineTransformerAndRefusesTheEmbeddingTransformer() {
    UnstructuredSourceContract source = source(UnstructuredMode.BLOB);
    assertEquals(Arrays.asList(QUARANTINE, GUARD), source.transformerChain(null));
    HoodieTransformPlanException refused = assertThrows(HoodieTransformPlanException.class,
        () -> source.transformerChain(Collections.singletonList(EMBEDDING)));
    assertTrue(refused.getMessage().contains("does not run with " + EMBEDDING), refused.getMessage());
  }

  @Test
  void embeddingSourceRunsTheEmbeddingTransformerAndRefusesTheQuarantineTransformer() {
    UnstructuredSourceContract source = source(UnstructuredMode.EMBEDDING);
    assertEquals(Arrays.asList(EMBEDDING, GUARD), source.transformerChain(null));
    List<String> named = Collections.singletonList(EMBEDDING);
    assertEquals(Arrays.asList(EMBEDDING, GUARD), source.transformerChain(named), "a job naming it once keeps its place");
    HoodieTransformPlanException refused = assertThrows(HoodieTransformPlanException.class,
        () -> source.transformerChain(Collections.singletonList(QUARANTINE)));
    assertTrue(refused.getMessage().contains("does not run with " + QUARANTINE), refused.getMessage());
  }

  public static class QuarantineSubclass extends BlobQuarantineTransformer {
    static final int VALUE = fail();

    private static int fail() {
      throw new IllegalStateException("initialized");
    }
  }

  public static class EmbeddingSubclass extends EmbeddingTableTransformer {
  }

  @Test
  void requiredSubclassRunsOnceAndKeepsItsFirstPosition() {
    UnstructuredSourceContract source = source(UnstructuredMode.BLOB);
    String child = QuarantineSubclass.class.getName();
    List<String> configured = Arrays.asList("before.Custom", "first:" + child, "again:" + QUARANTINE, "after.Custom");
    assertEquals(Arrays.asList("before.Custom", "first:" + child, "after.Custom", "UnstructuredSchemaGuard:" + GUARD),
        source.transformerChain(configured));
    assertEquals(4, configured.size(), "composition must not mutate the user's list");
  }

  @Test
  void namedChainGetsStableIdsForTheRequiredTransformerAndFinalGuard() {
    assertEquals(Arrays.asList("BlobQuarantineTransformer:" + QUARANTINE, "user:user.Custom", "UnstructuredSchemaGuard:" + GUARD),
        source(UnstructuredMode.BLOB).transformerChain(Collections.singletonList("user:user.Custom")));
  }

  @Test
  void addedIdsCannotCollideWithUserEntries() {
    UnstructuredSourceContract source = source(UnstructuredMode.BLOB);
    assertThrows(HoodieTransformPlanException.class,
        () -> source.transformerChain(Collections.singletonList("BlobQuarantineTransformer:user.Custom")));
    assertThrows(HoodieTransformPlanException.class,
        () -> source.transformerChain(Collections.singletonList("UnstructuredSchemaGuard:user.Custom")));
  }

  @Test
  void explicitGuardAndOtherModesSubclassAreRejected() {
    UnstructuredSourceContract source = source(UnstructuredMode.BLOB);
    assertThrows(HoodieTransformPlanException.class, () -> source.transformerChain(Collections.singletonList("guard:" + GUARD)));
    List<String> otherModesSubclass = Collections.singletonList("other:" + EmbeddingSubclass.class.getName());
    assertThrows(HoodieTransformPlanException.class, () -> source.transformerChain(otherModesSubclass));
  }

}
