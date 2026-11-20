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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The blob contract composes the required copy transformer and final guard around the configured entries. */
class TestUnstructuredTransformerChain {

  private static final String COPY = BlobQuarantineTransformer.class.getName();
  private static final String GUARD = UnstructuredSchemaGuard.class.getName();
  private static final String OTHER = "user.OtherTransformer";
  private static final String LAST = "user.LastTransformer";

  private static UnstructuredSourceContract contract() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    return new UnstructuredSourceContract(props);
  }

  @Test
  void requiredTransformerAndFinalGuardRunWhenTheJobConfiguresNone() {
    assertEquals(Arrays.asList(COPY, GUARD), contract().transformerChain(null));
    assertEquals(Arrays.asList(COPY, GUARD), contract().transformerChain(Collections.emptyList()));
  }

  @Test
  void requiredTransformerIsAddedBeforeConfiguredEntriesAndGuardAfterThem() {
    List<String> configured = Arrays.asList(OTHER, LAST);
    assertEquals(Arrays.asList(COPY, OTHER, LAST, GUARD), contract().transformerChain(configured));
    assertEquals(Arrays.asList(OTHER, LAST), configured, "composition must not mutate the configured entries");
  }

  @Test
  void jobNamingTheRequiredTransformerKeepsItsPositionAndConfiguredOrder() {
    List<String> configured = Arrays.asList(OTHER, COPY, LAST);
    assertEquals(Arrays.asList(OTHER, COPY, LAST, GUARD), contract().transformerChain(configured));
    assertEquals(Arrays.asList(OTHER, COPY, LAST), configured);
  }

  @Test
  void requiredTransformerNamedTwiceRunsOnceAtItsFirstPosition() {
    assertComposed(Arrays.asList(OTHER, COPY, LAST, COPY), Arrays.asList(OTHER, COPY, LAST, GUARD));
    assertComposed(Arrays.asList(COPY, OTHER, CopySubclass.class.getName(), COPY, LAST),
        Arrays.asList(COPY, OTHER, LAST, GUARD));
    assertComposed(Arrays.asList(OTHER, CopySubclass.class.getName(), COPY, LAST),
        Arrays.asList(OTHER, CopySubclass.class.getName(), LAST, GUARD));
    assertComposed(Arrays.asList(OTHER, COPY, " " + COPY + " ", LAST), Arrays.asList(OTHER, COPY, LAST, GUARD));
    assertComposed(Arrays.asList(OTHER, COPY, OTHER, COPY, LAST), Arrays.asList(OTHER, COPY, OTHER, LAST, GUARD));
    assertComposed(Arrays.asList("first:" + OTHER, "copy:" + COPY, "last:" + LAST, "duplicate:" + COPY),
        Arrays.asList("first:" + OTHER, "copy:" + COPY, "last:" + LAST, "UnstructuredSchemaGuard:" + GUARD));
    assertComposed(Arrays.asList("first:" + OTHER, "copy:" + COPY, "copy:" + COPY, "last:" + LAST),
        Arrays.asList("first:" + OTHER, "copy:" + COPY, "last:" + LAST, "UnstructuredSchemaGuard:" + GUARD));
  }

  private static void assertComposed(List<String> configured, List<String> expected) {
    List<String> before = new ArrayList<>(configured);
    assertEquals(expected, contract().transformerChain(configured));
    assertEquals(before, configured, "composition must not mutate the configured entries");
  }

  @Test
  void namedChainGetsStableIdsForTheRequiredTransformerAndFinalGuard() {
    assertEquals(Arrays.asList("BlobQuarantineTransformer:" + COPY, "other:" + OTHER, "last:" + LAST,
        "UnstructuredSchemaGuard:" + GUARD), contract().transformerChain(Arrays.asList("other:" + OTHER, "last:" + LAST)));
    assertEquals(Arrays.asList("other:" + OTHER, "copy:" + COPY, "UnstructuredSchemaGuard:" + GUARD),
        contract().transformerChain(Arrays.asList("other:" + OTHER, "copy:" + COPY)));
  }

  @Test
  void addedIdsCannotCollideWithConfiguredEntries() {
    HoodieTransformPlanException copy = assertThrows(HoodieTransformPlanException.class,
        () -> contract().transformerChain(Collections.singletonList("BlobQuarantineTransformer:" + OTHER)));
    assertTrue(copy.getMessage().contains("uses the id BlobQuarantineTransformer"), copy.getMessage());
    HoodieTransformPlanException guard = assertThrows(HoodieTransformPlanException.class,
        () -> contract().transformerChain(Collections.singletonList("UnstructuredSchemaGuard:" + OTHER)));
    assertTrue(guard.getMessage().contains("uses the id UnstructuredSchemaGuard"), guard.getMessage());
  }

  @Test
  void explicitFinalGuardIsRefusedInBothEntryForms() {
    for (String entry : Arrays.asList(GUARD, "guard:" + GUARD)) {
      HoodieTransformPlanException named = assertThrows(HoodieTransformPlanException.class,
          () -> contract().transformerChain(Arrays.asList(OTHER, entry)));
      assertTrue(named.getMessage().contains("runs " + GUARD + " last itself"), named.getMessage());
    }
  }

  @Test
  void subclassProbeDoesNotInitializeTheConfiguredClass() {
    String child = CopySubclass.class.getName();
    assertEquals(Arrays.asList(child, GUARD), contract().transformerChain(Collections.singletonList(child)));
    assertFalse(InitializationProbe.INITIALIZED.get(), "composition initialized the configured transformer class");
  }

  @Test
  void entryNamesItsTransformerInBothForms() {
    assertTrue(UnstructuredTransformerChain.names(COPY, BlobQuarantineTransformer.class));
    assertTrue(UnstructuredTransformerChain.names(" copy : " + CopySubclass.class.getName(), BlobQuarantineTransformer.class));
    assertFalse(UnstructuredTransformerChain.names(OTHER, BlobQuarantineTransformer.class));
  }

  /** Keeps initialization observable without reading the subclass after its initializer fails. */
  static final class InitializationProbe {
    static final AtomicBoolean INITIALIZED = new AtomicBoolean();
  }

  /** A configured subclass that composition must recognize without initializing it. */
  public static class CopySubclass extends BlobQuarantineTransformer {
    static {
      if (InitializationProbe.INITIALIZED.compareAndSet(false, true)) {
        throw new IllegalStateException("injected transformer initialization failure");
      }
    }
  }
}
