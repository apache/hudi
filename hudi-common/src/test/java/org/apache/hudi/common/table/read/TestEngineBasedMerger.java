package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEngineBasedMerger {
  private static final BufferedRecord<TestRecord> T1 = new BufferedRecord<>("key", 1L, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> T2 = new BufferedRecord<>("key", 2L, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> T3 = new BufferedRecord<>("key", 3L, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> HARD_DELETE = new BufferedRecord<>("key", 0L, new TestRecord(), 2, true);
  private static final BufferedRecord<TestRecord> T2_SOFT_DELETE = new BufferedRecord<>("key", 2L, new TestRecord(), 2, true);

  private final HoodieReaderContext<TestRecord> readerContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
  private final TypedProperties props = mock(TypedProperties.class);
  private final Schema readerSchema = mock(Schema.class);

  private static Stream<Arguments> commitTimeOrdering() {
    return Stream.of(
        // Validate commit time does not impact the ordering
        Arguments.of(Arrays.asList(T1, T3, T2), T2),
        // Validate hard delete does not impact the ordering
        Arguments.of(Arrays.asList(T1, HARD_DELETE, T2), T2));
  }

  @ParameterizedTest
  @MethodSource
  void commitTimeOrdering(List<BufferedRecord<TestRecord>> recordSequence, BufferedRecord expected) throws Exception {
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getSchemaHandler().getRequiredSchema()).thenReturn(readerSchema);
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, null, props, Option.empty());

    BufferedRecord<TestRecord> result = recordSequence.get(0);
    for (int i = 1; i < recordSequence.size(); i++) {
      BufferedRecord<TestRecord> current = recordSequence.get(i);
      result = merger.merge(Option.of(result), Option.of(current), false);
    }
    assertSame(expected, result);
  }


  private static Stream<Arguments> eventTimeOrdering() {
    return Stream.of(
        // Validate event time is used
        Arguments.of(Arrays.asList(T1, T3, T2), T3),
        // Validate hard delete is seen as most recent
        Arguments.of(Arrays.asList(T1, HARD_DELETE, T2), HARD_DELETE),
        // Validate soft delete is considered in order
        Arguments.of(Arrays.asList(T1, T2_SOFT_DELETE, T3), T3),
        Arguments.of(Arrays.asList(T3, T2_SOFT_DELETE, T1), T3));
  }

  @ParameterizedTest
  @MethodSource
  void eventTimeOrdering(List<BufferedRecord<TestRecord>> recordSequence, BufferedRecord expected) throws Exception {
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getSchemaHandler().getRequiredSchema()).thenReturn(readerSchema);
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, null, props, Option.empty());

    BufferedRecord<TestRecord> result = recordSequence.get(0);
    for (int i = 1; i < recordSequence.size(); i++) {
      BufferedRecord<TestRecord> current = recordSequence.get(i);
      result = merger.merge(Option.of(result), Option.of(current), false);
    }
    assertSame(expected, result);
  }


  private static class TestRecord {
    // placeholder class for ease of testing
  }
}
