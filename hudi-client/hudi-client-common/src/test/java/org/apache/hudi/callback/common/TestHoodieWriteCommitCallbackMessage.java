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

package org.apache.hudi.callback.common;

import org.apache.hudi.callback.util.HoodieWriteCommitCallbackUtil;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage.PrevFilePaths;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link HoodieWriteCommitCallbackMessage} contract: default (never-null)
 * collections, lazy one-shot resolution of {@code prevFilePaths} off the supplied file-system
 * view, and the Java-serialization round trip (the view cannot be shipped, so the resolved
 * paths must be materialized at the boundary and carried across in its place).
 */
public class TestHoodieWriteCommitCallbackMessage {

  private static final String COMMIT_TIME = "002";
  private static final String PARTITION = "2024/01/01";
  private static final String PREV_COMMIT = "001";
  private static final String PREV_PATH = "/tbl/" + PARTITION + "/f0_0-1-1_" + PREV_COMMIT + ".parquet";
  private static final String BOOTSTRAP_PATH = "/bootstrap/source/f0.parquet";

  private static List<HoodieWriteStat> updateStat() {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("f0");
    writeStat.setPartitionPath(PARTITION);
    writeStat.setPrevCommit(PREV_COMMIT);
    return Collections.singletonList(writeStat);
  }

  private static BaseFileOnlyView viewResolving(String prevBaseFilePath) {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "f0"))
        .thenReturn(Option.of(new HoodieBaseFile(prevBaseFilePath)));
    return view;
  }

  private static BaseFileOnlyView viewResolvingWithBootstrap(String prevBaseFilePath, String bootstrapPath) {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "f0"))
        .thenReturn(Option.of(new HoodieBaseFile(prevBaseFilePath, new BaseFile(bootstrapPath))));
    return view;
  }

  @Test
  public void callbackMessageDefaultsCollectionsToEmpty() {
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", Collections.emptyList());

    assertFalse(message.getCommitActionType().isPresent());
    assertFalse(message.getExtraMetadata().isPresent());
    assertTrue(message.getPrevFilePaths().isEmpty(), "prevFilePaths must default to an empty map, never null");
    assertTrue(message.getExtraContext().isEmpty(), "extraContext must default to an empty map, never null");
  }

  @Test
  public void callbackMessageResolvesPrevFilePathsFromViewAndRetainsContext() {
    Map<String, String> extraContext = Collections.singletonMap("file_id", "f0");

    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", updateStat(),
        Option.of("commit"), Option.empty(), () -> viewResolving(PREV_PATH), extraContext);

    assertEquals("commit", message.getCommitActionType().get());
    PrevFilePaths resolved = message.getPrevFilePaths().get("f0");
    assertEquals(PREV_PATH, resolved.getBaseFilePath());
    assertEquals(extraContext, message.getExtraContext());
  }

  @Test
  public void nullFileSystemViewSupplierYieldsEmptyPrevFilePaths() {
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", updateStat(),
        Option.of("commit"), Option.empty(), null, Collections.emptyMap());

    assertTrue(message.getPrevFilePaths().isEmpty(),
        "a message built without a file-system view must yield an empty map, never null");
  }

  @Test
  public void prevFilePathsAreResolvedLazilyAndMemoized() {
    AtomicInteger viewLookups = new AtomicInteger();
    BaseFileOnlyView view = viewResolving(PREV_PATH);
    Supplier<BaseFileOnlyView> viewSupplier = () -> {
      viewLookups.incrementAndGet();
      return view;
    };

    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", updateStat(),
        Option.empty(), Option.empty(), viewSupplier, Collections.emptyMap());

    // Constructing the message must not touch the file-system view.
    assertEquals(0, viewLookups.get(),
        "prevFilePaths must not be resolved until a consumer reads them");
    verify(view, never()).getBaseFileOn(anyString(), anyString(), anyString());

    assertEquals(PREV_PATH, message.getPrevFilePaths().get("f0").getBaseFilePath());
    // A second read must reuse the memoized result rather than resolve again.
    assertEquals(PREV_PATH, message.getPrevFilePaths().get("f0").getBaseFilePath());
    assertEquals(1, viewLookups.get(), "prevFilePaths must be resolved at most once and memoized");
    verify(view).getBaseFileOn(PARTITION, PREV_COMMIT, "f0");
  }

  @Test
  public void javaSerializationResolvesAndPreservesPrevFilePaths() throws IOException, ClassNotFoundException {
    AtomicInteger viewLookups = new AtomicInteger();
    // The view supplier cannot be shipped, so writeObject has to materialize the paths first.
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", updateStat(),
        Option.of("commit"), Option.empty(),
        () -> {
          viewLookups.incrementAndGet();
          return viewResolvingWithBootstrap(PREV_PATH, BOOTSTRAP_PATH);
        },
        Collections.emptyMap());

    assertEquals(0, viewLookups.get(), "building the message must not touch the file-system view");

    HoodieWriteCommitCallbackMessage roundTripped = serializeAndDeserialize(message);

    assertEquals(1, viewLookups.get(), "serialization must force resolution exactly once");
    assertEquals(COMMIT_TIME, roundTripped.getCommitTime());
    assertEquals("commit", roundTripped.getCommitActionType().get());
    assertEquals(1, roundTripped.getHoodieWriteStat().size());
    assertEquals(PREV_PATH, roundTripped.getPrevFilePaths().get("f0").getBaseFilePath());
    assertEquals(BOOTSTRAP_PATH, roundTripped.getPrevFilePaths().get("f0").getBootstrapBaseFilePath(),
        "the bootstrap source path must survive the round trip too");
  }

  @Test
  public void jsonPayloadExposesPrevFilePathsAndNotTheResolver() {
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", updateStat(),
        Option.of("commit"), Option.empty(), () -> viewResolving(PREV_PATH), Collections.emptyMap());

    // This is the payload the built-in HTTP/Kafka/Pulsar callbacks put on the wire.
    String json = HoodieWriteCommitCallbackUtil.convertToJsonString(message);

    assertTrue(json.contains("\"prevFilePaths\""), "prevFilePaths must be part of the callback payload");
    assertTrue(json.contains(PREV_PATH), "Jackson must see the resolved paths, not the lazy holder");
    assertFalse(json.contains("prevFilePathsResolver"),
        "the lazy resolver is an implementation detail and must never reach the payload");
  }

  private static HoodieWriteCommitCallbackMessage serializeAndDeserialize(
      HoodieWriteCommitCallbackMessage message) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(message);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (HoodieWriteCommitCallbackMessage) in.readObject();
    }
  }
}
