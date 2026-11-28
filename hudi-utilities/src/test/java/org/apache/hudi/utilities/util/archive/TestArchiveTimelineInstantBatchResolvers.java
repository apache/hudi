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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link ArchiveTimelineInstantBatchResolvers}: the layout-keyed choice between
 * {@link V1ArchiveTimelineInstantBatchResolver} and {@link V2ArchiveTimelineInstantBatchResolver},
 * and that an unreadable layout version refuses rather than guesses v1.
 */
class TestArchiveTimelineInstantBatchResolvers {

  /**
   * Parameterised over <b>every</b> table version carrying layout 1, and that is the point rather
   * than thoroughness for its own sake: version seven carries layout 1 too, so a gate written as
   * {@code version == SIX} would pass on {@code SIX} alone and silently disable the archived answer
   * on every version-seven table. Keyed on the timeline layout version, both pass.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "SEVEN"})
  void tableBelowLayoutTwoGetsTheV1Resolver(HoodieTableVersion version) {
    HoodieTableMetaClient metaClient = bareMetaClient(version);

    assertTrue(ArchiveTimelineInstantBatchResolvers.readsV1Archive(metaClient));
    ArchiveTimelineInstantBatchResolver<String> resolver =
        ArchiveTimelineInstantBatchResolvers.forTable(metaClient, projector());
    assertTrue(resolver instanceof V1ArchiveTimelineInstantBatchResolver,
        "a table below layout 2 must get the v1 resolver");
  }

  /**
   * The other side of the same gate, over every table version carrying layout 2. {@code NINE} is
   * here because a gate that merely happens to exclude {@code EIGHT} would let a version-nine table
   * reach the v1 resolver, which reads {@code .commits_.archive.N} files: it would list none of them
   * on an LSM archive and report a clean absence having read no archive whatsoever, authorising a
   * deletion on evidence it never gathered.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE"})
  void tableAtLayoutTwoOrAboveGetsTheV2Resolver(HoodieTableVersion version) {
    HoodieTableMetaClient metaClient = bareMetaClient(version);

    assertFalse(ArchiveTimelineInstantBatchResolvers.readsV1Archive(metaClient));
    ArchiveTimelineInstantBatchResolver<String> resolver =
        ArchiveTimelineInstantBatchResolvers.forTable(metaClient, projector());
    assertTrue(resolver instanceof V2ArchiveTimelineInstantBatchResolver,
        "a table at or above layout 2 must get the v2 resolver");
  }

  @Test
  void unreadableLayoutVersionIsNotTreatedAsV1() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    Mockito.when(metaClient.getTableConfig()).thenThrow(new RuntimeException("table config unreadable"));

    // Not knowing the layout means not knowing whether the archive can be read at all, which is a
    // reason to refuse (false) rather than to search the wrong thing (true).
    assertFalse(ArchiveTimelineInstantBatchResolvers.readsV1Archive(metaClient));
  }

  @Test
  void v2ResolverThrowsRatherThanGuessing() {
    V2ArchiveTimelineInstantBatchResolver<String> resolver = new V2ArchiveTimelineInstantBatchResolver<>();
    Set<String> targets = Collections.singleton("20260101000000000");

    assertThrows(UnsupportedOperationException.class, () -> resolver.warmUp(targets));
  }

  private static HoodieTableMetaClient bareMetaClient(HoodieTableVersion tableVersion) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    // Only needed so V1ArchiveTimelineInstantBatchResolver's constructor -- which reads these
    // eagerly -- does not NPE when forTable builds one; unused by the v2 branch.
    Mockito.when(metaClient.getStorage()).thenReturn(Mockito.mock(HoodieStorage.class));
    Mockito.when(metaClient.getArchivePath()).thenReturn(new StoragePath("/tmp/table/.hoodie/archived"));
    return metaClient;
  }

  private static ArchivedRecordProjector<String> projector() {
    return new ArchivedRecordProjector<String>() {
      @Override
      public String project(org.apache.avro.generic.GenericRecord record) {
        return "";
      }

      @Override
      public String merge(String first, String second) {
        return first;
      }
    };
  }
}
