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

import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage.PrevFilePaths;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link HoodieWriteCommitCallbackMessage} contract: default (never-null)
 * collections, lazy one-shot resolution of {@code prevFilePaths}, and the Java-serialization
 * round trip that has to carry the resolved paths across (the lazy holder itself is transient).
 */
public class TestHoodieWriteCommitCallbackMessage {

  private static final String COMMIT_TIME = "001";

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
  public void callbackMessageRetainsPrevFilePathsAndContext() {
    Map<String, PrevFilePaths> prevFilePaths =
        Collections.singletonMap("f0", new PrevFilePaths("/tbl/prev.parquet", null));
    Map<String, String> extraContext = Collections.singletonMap("file_id", "f0");

    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", Collections.emptyList(),
        Option.of("commit"), Option.empty(), () -> prevFilePaths, extraContext);

    assertEquals("commit", message.getCommitActionType().get());
    assertEquals(prevFilePaths, message.getPrevFilePaths());
    assertEquals(extraContext, message.getExtraContext());
  }

  @Test
  public void prevFilePathsAreResolvedLazilyAndMemoized() {
    AtomicInteger resolveCalls = new AtomicInteger();
    Map<String, PrevFilePaths> resolved =
        Collections.singletonMap("f0", new PrevFilePaths("/tbl/prev.parquet", null));
    Supplier<Map<String, PrevFilePaths>> supplier = () -> {
      resolveCalls.incrementAndGet();
      return resolved;
    };

    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", Collections.emptyList(),
        Option.empty(), Option.empty(), supplier, Collections.emptyMap());

    // Constructing the message must not resolve prev file paths (no FileSystemView access).
    assertEquals(0, resolveCalls.get(),
        "prevFilePaths must not be resolved until a consumer reads them");

    assertEquals(resolved, message.getPrevFilePaths());
    // A second read must reuse the memoized result rather than resolve again.
    assertEquals(resolved, message.getPrevFilePaths());
    assertEquals(1, resolveCalls.get(), "prevFilePaths must be resolved at most once and memoized");
  }

  @Test
  public void prevFilePathsSurviveJavaSerialization() throws IOException, ClassNotFoundException {
    Map<String, PrevFilePaths> prevFilePaths = Collections.singletonMap(
        "f0", new PrevFilePaths("/tbl/prev.parquet", "/bootstrap/source/f0.parquet"));
    // A supplier that is itself unserializable: writeObject must resolve it away, not ship it.
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", Collections.emptyList(),
        Option.of("commit"), Option.empty(), () -> prevFilePaths, Collections.emptyMap());

    HoodieWriteCommitCallbackMessage roundTripped = serializeAndDeserialize(message);

    assertEquals(COMMIT_TIME, roundTripped.getCommitTime());
    assertEquals(1, roundTripped.getPrevFilePaths().size());
    assertEquals("/tbl/prev.parquet", roundTripped.getPrevFilePaths().get("f0").getBaseFilePath());
    assertEquals("/bootstrap/source/f0.parquet",
        roundTripped.getPrevFilePaths().get("f0").getBootstrapBaseFilePath());
  }

  @Test
  public void unresolvedPrevFilePathsSerializeAsEmpty() throws IOException, ClassNotFoundException {
    HoodieWriteCommitCallbackMessage message = new HoodieWriteCommitCallbackMessage(
        COMMIT_TIME, "table", "/base", Collections.emptyList());

    assertTrue(serializeAndDeserialize(message).getPrevFilePaths().isEmpty(),
        "a message with nothing to resolve must round trip to an empty map, never null");
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
