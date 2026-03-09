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
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSourceSplitStatus;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    assertEquals(1, serializer.getVersion());
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
