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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitSerializer;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSourceSplitStatus;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieEnumeratorStateSerializer}.
 */
public class TestHoodieEnumeratorStateSerializer {

  private final HoodieEnumeratorStateSerializer serializer = new HoodieEnumeratorStateSerializer();

  @Test
  public void testSerializeAndDeserializeEmptyState() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.empty(),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    assertNotNull(serialized);
    assertTrue(serialized.length > 0);

    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(0, deserialized.getPendingSplitStates().size());
    assertFalse(deserialized.getLastEnumeratedInstant().isPresent());
    assertFalse(deserialized.getLastEnumeratedInstantOffset().isPresent());
  }

  @Test
  public void testSerializeAndDeserializeStateWithOneSplit() throws IOException {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplitState splitState = new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED);

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.singletonList(splitState),
        Option.of("20240122120000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(1, deserialized.getPendingSplitStates().size());
    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("20240122120000", deserialized.getLastEnumeratedInstant().get());
    assertFalse(deserialized.getLastEnumeratedInstantOffset().isPresent());

    List<HoodieSourceSplitState> splitStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, splitStates.get(0).getStatus());
    assertEquals(split, splitStates.get(0).getSplit());
  }

  @Test
  public void testSerializeAndDeserializeStateWithMultipleSplits() throws IOException {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3", "/partition3");

    List<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.UNASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.ASSIGNED),
        new HoodieSourceSplitState(split3, HoodieSourceSplitStatus.COMPLETED)
    );

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.of("offset-123")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(3, deserialized.getPendingSplitStates().size());
    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("20240122120000", deserialized.getLastEnumeratedInstant().get());
    assertTrue(deserialized.getLastEnumeratedInstantOffset().isPresent());
    assertEquals("offset-123", deserialized.getLastEnumeratedInstantOffset().get());

    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, deserializedStates.get(0).getStatus());
    assertEquals(HoodieSourceSplitStatus.ASSIGNED, deserializedStates.get(1).getStatus());
    assertEquals(HoodieSourceSplitStatus.COMPLETED, deserializedStates.get(2).getStatus());
  }

  @Test
  public void testSerializeAndDeserializeStateWithOnlyLastInstant() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20240122120000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("20240122120000", deserialized.getLastEnumeratedInstant().get());
    assertFalse(deserialized.getLastEnumeratedInstantOffset().isPresent());
  }

  @Test
  public void testSerializeAndDeserializeStateWithOnlyLastOffset() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.empty(),
        Option.of("offset-456")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertFalse(deserialized.getLastEnumeratedInstant().isPresent());
    assertTrue(deserialized.getLastEnumeratedInstantOffset().isPresent());
    assertEquals("offset-456", deserialized.getLastEnumeratedInstantOffset().get());
  }

  @Test
  public void testSerializeAndDeserializeStateWithBothInstantAndOffset() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20240122120000"),
        Option.of("offset-789")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("20240122120000", deserialized.getLastEnumeratedInstant().get());
    assertTrue(deserialized.getLastEnumeratedInstantOffset().isPresent());
    assertEquals("offset-789", deserialized.getLastEnumeratedInstantOffset().get());
  }

  @Test
  public void testSerializeAndDeserializeComplexState() throws IOException {
    List<HoodieSourceSplitState> splitStates = new ArrayList<>();

    // Create 10 splits with different states
    for (int i = 0; i < 10; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file-" + i, "/partition-" + i);
      if (i % 3 == 0) {
        split.updatePosition(i, i * 100L);
      }
      HoodieSourceSplitStatus status;
      if (i % 3 == 0) {
        status = HoodieSourceSplitStatus.UNASSIGNED;
      } else if (i % 3 == 1) {
        status = HoodieSourceSplitStatus.ASSIGNED;
      } else {
        status = HoodieSourceSplitStatus.COMPLETED;
      }
      splitStates.add(new HoodieSourceSplitState(split, status));
    }

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122153045678"),
        Option.of("complex-offset-with-uuid-12345")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(10, deserialized.getPendingSplitStates().size());
    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("20240122153045678", deserialized.getLastEnumeratedInstant().get());
    assertTrue(deserialized.getLastEnumeratedInstantOffset().isPresent());
    assertEquals("complex-offset-with-uuid-12345", deserialized.getLastEnumeratedInstantOffset().get());

    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    for (int i = 0; i < 10; i++) {
      assertEquals(splitStates.get(i).getSplit(), deserializedStates.get(i).getSplit());
      assertEquals(splitStates.get(i).getStatus(), deserializedStates.get(i).getStatus());
    }
  }

  @Test
  public void testSerializeAndDeserializeWithDifferentSplitStatuses() throws IOException {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3", "/partition3");

    Collection<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.UNASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.ASSIGNED),
        new HoodieSourceSplitState(split3, HoodieSourceSplitStatus.COMPLETED)
    );

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, deserializedStates.get(0).getStatus());
    assertEquals(HoodieSourceSplitStatus.ASSIGNED, deserializedStates.get(1).getStatus());
    assertEquals(HoodieSourceSplitStatus.COMPLETED, deserializedStates.get(2).getStatus());
  }

  @Test
  public void testGetVersion() {
    assertEquals(2, serializer.getVersion());
  }

  @Test
  public void testSerializationIdempotency() throws IOException {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplitState splitState = new HoodieSourceSplitState(split, HoodieSourceSplitStatus.ASSIGNED);

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.singletonList(splitState),
        Option.of("20240122120000"),
        Option.of("offset-123")
    );

    byte[] serialized1 = serializer.serialize(original);
    byte[] serialized2 = serializer.serialize(original);

    // Serializing the same object twice should produce identical results
    assertEquals(serialized1.length, serialized2.length);
  }

  @Test
  public void testDeserializationProducesEquivalentState() throws IOException {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");

    Collection<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.UNASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.ASSIGNED)
    );

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.of("offset-123")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    // Verify all fields match
    assertEquals(original.getPendingSplitStates().size(), deserialized.getPendingSplitStates().size());
    assertEquals(original.getLastEnumeratedInstant(), deserialized.getLastEnumeratedInstant());
    assertEquals(original.getLastEnumeratedInstantOffset(), deserialized.getLastEnumeratedInstantOffset());
  }

  @Test
  public void testSerializeAndDeserializeWithConsumedSplits() throws IOException {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    split1.consume();
    split1.consume();

    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");
    split2.updatePosition(5, 500L);

    Collection<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.ASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.COMPLETED)
    );

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(2L, deserializedStates.get(0).getSplit().getConsumed());
    assertEquals(500L, deserializedStates.get(1).getSplit().getConsumed());
    assertEquals(5, deserializedStates.get(1).getSplit().getFileOffset());
  }

  @Test
  public void testSerializeWithVeryLargeNumberOfSplits() throws IOException {
    List<HoodieSourceSplitState> splitStates = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      HoodieSourceSplit split = createTestSplit(i, "file-" + i, "/partition-" + (i % 10));
      HoodieSourceSplitStatus status = HoodieSourceSplitStatus.values()[i % 3];
      splitStates.add(new HoodieSourceSplitState(split, status));
    }

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.of("offset-large-batch")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(1000, deserialized.getPendingSplitStates().size());
  }

  @Test
  public void testSerializeWithVeryLongInstantStrings() throws IOException {
    StringBuilder longInstant = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longInstant.append("timestamp-segment-");
    }

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of(longInstant.toString()),
        Option.of(longInstant.toString())
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(original.getLastEnumeratedInstant(), deserialized.getLastEnumeratedInstant());
    assertEquals(original.getLastEnumeratedInstantOffset(), deserialized.getLastEnumeratedInstantOffset());
  }

  @Test
  public void testSerializeWithSpecialCharactersInInstants() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("instant-with-特殊字符-émojis-🎉"),
        Option.of("offset-with-\t-tabs-\n-newlines-\r\n-carriage")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(original.getLastEnumeratedInstant(), deserialized.getLastEnumeratedInstant());
    assertEquals(original.getLastEnumeratedInstantOffset(), deserialized.getLastEnumeratedInstantOffset());
  }

  @Test
  public void testSerializeWithEmptyInstantStrings() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of(""),
        Option.of("")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertTrue(deserialized.getLastEnumeratedInstant().isPresent());
    assertEquals("", deserialized.getLastEnumeratedInstant().get());
    assertTrue(deserialized.getLastEnumeratedInstantOffset().isPresent());
    assertEquals("", deserialized.getLastEnumeratedInstantOffset().get());
  }

  @Test
  public void testSerializeWithAllStatusTypes() throws IOException {
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/partition1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2", "/partition2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3", "/partition3");

    Collection<HoodieSourceSplitState> splitStates = Arrays.asList(
        new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.UNASSIGNED),
        new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.ASSIGNED),
        new HoodieSourceSplitState(split3, HoodieSourceSplitStatus.COMPLETED)
    );

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, deserializedStates.get(0).getStatus());
    assertEquals(HoodieSourceSplitStatus.ASSIGNED, deserializedStates.get(1).getStatus());
    assertEquals(HoodieSourceSplitStatus.COMPLETED, deserializedStates.get(2).getStatus());
  }

  @Test
  public void testRoundTripSerializationMultipleTimes() throws IOException {
    HoodieSourceSplit split = createTestSplit(1, "file1", "/partition1");
    split.updatePosition(5, 100L);

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.singletonList(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.ASSIGNED)),
        Option.of("20240122120000"),
        Option.of("offset-123")
    );

    // Serialize and deserialize multiple times
    HoodieSplitEnumeratorState current = original;
    for (int i = 0; i < 10; i++) {
      byte[] serialized = serializer.serialize(current);
      current = serializer.deserialize(serializer.getVersion(), serialized);
    }

    assertEquals(original.getPendingSplitStates().size(), current.getPendingSplitStates().size());
    assertEquals(original.getLastEnumeratedInstant(), current.getLastEnumeratedInstant());
    assertEquals(original.getLastEnumeratedInstantOffset(), current.getLastEnumeratedInstantOffset());
  }

  @Test
  public void testSerializeStateWithMixedSplitStates() throws IOException {
    List<HoodieSourceSplitState> splitStates = new ArrayList<>();

    // Add splits with different characteristics
    HoodieSourceSplit split1 = createTestSplit(1, "file1", "/p1");
    split1.consume();
    splitStates.add(new HoodieSourceSplitState(split1, HoodieSourceSplitStatus.ASSIGNED));

    HoodieSourceSplit split2 = new HoodieSourceSplit(2, null,
        Option.of(Arrays.asList("log1", "log2")), "/table", "/p2", "payload_combine", "", "file2", Option.empty());
    splitStates.add(new HoodieSourceSplitState(split2, HoodieSourceSplitStatus.UNASSIGNED));

    HoodieSourceSplit split3 = createTestSplit(3, "file3", "/p3");
    split3.updatePosition(Integer.MAX_VALUE, Long.MAX_VALUE);
    splitStates.add(new HoodieSourceSplitState(split3, HoodieSourceSplitStatus.COMPLETED));

    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        splitStates,
        Option.of("20240122120000"),
        Option.of("offset-mixed")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(3, deserialized.getPendingSplitStates().size());
    List<HoodieSourceSplitState> deserializedStates = new ArrayList<>(deserialized.getPendingSplitStates());
    assertEquals(1L, deserializedStates.get(0).getSplit().getConsumed());
    assertFalse(deserializedStates.get(1).getSplit().getBasePath().isPresent());
    assertEquals(Integer.MAX_VALUE, deserializedStates.get(2).getSplit().getFileOffset());
  }

  @Test
  public void testSerializeAndDeserializeCommitRange() throws IOException {
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.empty(),
        Option.empty(),
        Option.of("20260226000000"),
        Option.of("20260227000000")
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(Option.of("20260226000000"), deserialized.getReadStartCommit());
    assertEquals(Option.of("20260227000000"), deserialized.getReadEndCommit());
  }

  @Test
  public void testRecordedButUnsetCommitRangeRoundTrips() throws IOException {
    // A bounded read with neither option configured records the empty string rather than an absent
    // Option, so a restore can tell "recorded but unset" apart from "not recorded at all".
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.empty(),
        Option.empty(),
        Option.of(""),
        Option.of("")
    );

    HoodieSplitEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

    assertEquals(Option.of(""), deserialized.getReadStartCommit());
    assertEquals(Option.of(""), deserialized.getReadEndCommit());
  }

  @Test
  public void testCommitRangeDefaultsEmptyViaLegacyConstructor() throws IOException {
    // The 3-arg constructor (streaming enumerator + pre-existing call sites) records no range; it
    // must round-trip as absent so the restore-time range check is skipped.
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20240122120000"),
        Option.empty()
    );

    HoodieSplitEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

    assertFalse(deserialized.getReadStartCommit().isPresent());
    assertFalse(deserialized.getReadEndCommit().isPresent());
  }

  @Test
  public void testDeserializeVersion1Payload() throws IOException {
    // A VERSION 1 checkpoint: no nested split-serializer version at the head, no commit range at the
    // tail. It must still restore, with its splits intact and the range absent.
    HoodieSourceSplit split = createTestSplit(7, "file7", "/partition7");
    byte[] v1Bytes = serializeAsVersion1(
        Collections.singletonList(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.ASSIGNED)),
        Option.of("20240122120000"));

    HoodieSplitEnumeratorState deserialized = serializer.deserialize(1, v1Bytes);

    assertEquals(1, deserialized.getPendingSplitStates().size());
    HoodieSourceSplitState restored = deserialized.getPendingSplitStates().iterator().next();
    assertEquals(7, restored.getSplit().getSplitNum());
    assertEquals("file7", restored.getSplit().getFileId());
    assertEquals(HoodieSourceSplitStatus.ASSIGNED, restored.getStatus());
    assertEquals(Option.of("20240122120000"), deserialized.getLastEnumeratedInstant());
    assertFalse(deserialized.getReadStartCommit().isPresent());
    assertFalse(deserialized.getReadEndCommit().isPresent());
  }

  @Test
  public void testNestedSplitVersionIsRecordedNotInheritedFromOuterVersion() throws IOException {
    // The outer state format and the nested split format version independently. VERSION 2 records
    // the split serializer's own version in the payload so the splits are decoded with the version
    // that wrote them, rather than with whatever the outer version happens to be.
    HoodieSourceSplit split = createTestSplit(3, "file3", "/partition3");
    HoodieSplitEnumeratorState original = new HoodieSplitEnumeratorState(
        Collections.singletonList(new HoodieSourceSplitState(split, HoodieSourceSplitStatus.UNASSIGNED)),
        Option.empty(),
        Option.empty(),
        Option.of("20260226000000"),
        Option.of("")
    );

    byte[] serialized = serializer.serialize(original);

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
      assertEquals(new HoodieSourceSplitSerializer().getVersion(), in.readInt(),
          "VERSION 2 payloads must lead with the nested split serializer version");
    }
    // And the payload still round-trips end to end with that leading int in place.
    HoodieSplitEnumeratorState deserialized = serializer.deserialize(2, serialized);
    assertEquals(1, deserialized.getPendingSplitStates().size());
    assertEquals("file3", deserialized.getPendingSplitStates().iterator().next().getSplit().getFileId());
    assertEquals(Option.of("20260226000000"), deserialized.getReadStartCommit());
  }

  @Test
  public void testDeserializeRejectsNewerVersion() throws IOException {
    byte[] serialized = serializer.serialize(new HoodieSplitEnumeratorState(
        Collections.emptyList(), Option.empty(), Option.empty()));

    IOException ex = assertThrows(IOException.class,
        () -> serializer.deserialize(serializer.getVersion() + 1, serialized));
    assertTrue(ex.getMessage().contains("newer serializer version"));
  }

  /**
   * Writes the VERSION 1 payload layout: split states, then lastEnumeratedInstant and
   * lastEnumeratedInstantOffset. No nested split-serializer version, no commit range.
   */
  private byte[] serializeAsVersion1(
      List<HoodieSourceSplitState> splitStates, Option<String> lastEnumeratedInstant) throws IOException {
    HoodieSourceSplitSerializer splitSerializer = new HoodieSourceSplitSerializer();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(splitStates.size());
      for (HoodieSourceSplitState splitState : splitStates) {
        byte[] splitBytes = splitSerializer.serialize(splitState.getSplit());
        out.writeInt(splitBytes.length);
        out.write(splitBytes);
        out.writeUTF(splitState.getStatus().name());
      }
      out.writeBoolean(lastEnumeratedInstant.isPresent());
      if (lastEnumeratedInstant.isPresent()) {
        out.writeUTF(lastEnumeratedInstant.get());
      }
      out.writeBoolean(false);
      out.flush();
      return baos.toByteArray();
    }
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit(int splitNum, String fileId, String partitionPath) {
    return new HoodieSourceSplit(
        splitNum,
        "base-path-" + splitNum,
        Option.of(Collections.emptyList()),
        "/test/table",
        partitionPath,
        "read_optimized",
        "19700101000000000",
        fileId,
        Option.empty()
    );
  }
}
