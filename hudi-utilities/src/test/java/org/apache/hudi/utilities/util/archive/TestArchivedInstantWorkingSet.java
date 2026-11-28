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

package org.apache.hudi.utilities.util.archive;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestArchivedInstantWorkingSet {

  private static Set<String> targets(String... instants) {
    return new HashSet<>(Arrays.asList(instants));
  }

  @Test
  void allIndeterminateAnswersIndeterminateForEveryTarget() {
    ArchivedInstantWorkingSet<Set<String>> set =
        ArchivedInstantWorkingSet.allIndeterminate(targets("001", "002"));

    assertEquals(InstantSearchState.INDETERMINATE, set.stateOf("001"));
    assertEquals(InstantSearchState.INDETERMINATE, set.stateOf("002"));
    assertFalse(set.contentOf("001").isPresent());
  }

  @Test
  void allNotFoundAnswersNotFoundForEveryTarget() {
    ArchivedInstantWorkingSet<Set<String>> set =
        ArchivedInstantWorkingSet.allNotFound(targets("001"));

    assertEquals(InstantSearchState.NOT_FOUND, set.stateOf("001"));
    assertFalse(set.contentOf("001").isPresent());
  }

  @Test
  void foundCarriesItsProjectedContent() {
    ArchivedInstantWorkingSet.Builder<Set<String>> builder =
        ArchivedInstantWorkingSet.builder(targets("001", "002", "003"));
    builder.found("001", Collections.singleton("f1.parquet"));
    builder.notFound("002");
    builder.indeterminate("003");
    ArchivedInstantWorkingSet<Set<String>> set = builder.build();

    assertEquals(InstantSearchState.FOUND, set.stateOf("001"));
    assertTrue(set.contentOf("001").isPresent());
    assertEquals(Collections.singleton("f1.parquet"), set.contentOf("001").get());
    assertEquals(InstantSearchState.NOT_FOUND, set.stateOf("002"));
    assertFalse(set.contentOf("002").isPresent());
    assertEquals(InstantSearchState.INDETERMINATE, set.stateOf("003"));
  }

  @Test
  void targetTheBuilderNeverDecidedIsIndeterminate() {
    // A resolver that returns without deciding a target has not proven anything about it.
    ArchivedInstantWorkingSet<Set<String>> set =
        ArchivedInstantWorkingSet.<Set<String>>builder(targets("001")).build();

    assertEquals(InstantSearchState.INDETERMINATE, set.stateOf("001"));
  }

  @Test
  void askingAboutANonTargetIsACallerBug() {
    ArchivedInstantWorkingSet<Set<String>> set =
        ArchivedInstantWorkingSet.allNotFound(targets("001"));

    assertThrows(IllegalArgumentException.class, () -> set.stateOf("999"));
    assertThrows(IllegalArgumentException.class, () -> set.contentOf("999"));
  }

  @Test
  void targetsAreReadOnly() {
    ArchivedInstantWorkingSet<Set<String>> set =
        ArchivedInstantWorkingSet.allNotFound(targets("001"));

    assertThrows(UnsupportedOperationException.class, () -> set.targets().add("002"));
  }
}
