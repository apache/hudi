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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KinesisSource.ShardRecordIterator}, focusing on fetchNextPage retry logic,
 * adaptive throttle behaviour, and checkpoint correctness.
 */
class TestShardRecordIterator {

  private static final String SHARD_ID = "shardId-000000000000";
  private static final String INITIAL_ITER = "iter-initial";
  private static final String NEXT_ITER = "iter-next";

  // Set sleep intervals to 0 so tests do not block on the between-page delay.
  private static final long INTERVAL_MS = 0;
  // Set backoff base to 0 so throttle retries do not add significant sleep.
  // Note: up to 499 ms of jitter is still added per retry by the production code.
  private static final long RETRY_INITIAL_MS = 0;
  private static final long RETRY_MAX_MS = 0;
  // Large enough to never trigger during normal test execution.
  private static final long THROTTLE_TIMEOUT_LARGE = 30_000;

  private KinesisSource.ShardRecordIterator iterator(KinesisClient client,
      int maxRecordsPerRequest, long maxTotalRecords, long throttleTimeoutMs) {
    return new KinesisSource.ShardRecordIterator(INITIAL_ITER, client, SHARD_ID,
        maxRecordsPerRequest, INTERVAL_MS, maxTotalRecords, /* enableDeaggregation */ false,
        RETRY_INITIAL_MS, RETRY_MAX_MS, throttleTimeoutMs);
  }

  private static Record record(String sequenceNumber) {
    return Record.builder()
        .data(SdkBytes.fromUtf8String("{\"id\":1}"))
        .sequenceNumber(sequenceNumber)
        .partitionKey("pk")
        .approximateArrivalTimestamp(Instant.now())
        .build();
  }

  private static GetRecordsResponse response(List<Record> records, String nextShardIterator,
      long millisBehindLatest) {
    return GetRecordsResponse.builder()
        .records(records)
        .nextShardIterator(nextShardIterator)
        .millisBehindLatest(millisBehindLatest)
        .build();
  }

  private static ProvisionedThroughputExceededException throttleEx() {
    return ProvisionedThroughputExceededException.builder().message("throttled").build();
  }

  // -------------------------------------------------------------------------
  // Normal read scenarios
  // -------------------------------------------------------------------------

  @Test
  void testNormalReadReturnsAllRecordsAndSetsLastSeq() {
    KinesisClient client = mock(KinesisClient.class);
    List<Record> records = Arrays.asList(record("seq1"), record("seq2"), record("seq3"));
    when(client.getRecords(isA(GetRecordsRequest.class))).thenReturn(response(records, NEXT_ITER, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);

    List<Record> collected = new ArrayList<>();
    while (it.hasNext()) {
      collected.add(it.next());
    }

    assertEquals(3, collected.size());
    assertEquals("seq1", collected.get(0).sequenceNumber());
    assertEquals("seq3", collected.get(2).sequenceNumber());
    assertEquals("seq3", it.getLastSequenceNumber().get());
    assertFalse(it.isReachedEndOfShard());
  }

  @Test
  void testMillisBehindLatestZeroStopsAfterCurrentPage() {
    KinesisClient client = mock(KinesisClient.class);
    // Page 1: millisBehindLatest=0 → no second page should be fetched.
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenReturn(response(Arrays.asList(record("seq1"), record("seq2")), NEXT_ITER, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);
    while (it.hasNext()) {
      it.next();
    }

    verify(client, times(1)).getRecords(isA(GetRecordsRequest.class));
  }

  @Test
  void testNullNextShardIteratorSetsReachedEndOfShard() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenReturn(response(Collections.singletonList(record("seq1")), null, 100L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);
    while (it.hasNext()) {
      it.next();
    }

    assertTrue(it.isReachedEndOfShard());
    assertEquals("seq1", it.getLastSequenceNumber().get());
  }

  @Test
  void testMaxTotalRecordsStopsFetchingEarlyWithoutExtraPageCall() {
    KinesisClient client = mock(KinesisClient.class);
    List<Record> page = Arrays.asList(
        record("seq1"), record("seq2"), record("seq3"), record("seq4"), record("seq5"));
    // millisBehindLatest > 0: would fetch another page if limit were not reached.
    when(client.getRecords(isA(GetRecordsRequest.class))).thenReturn(response(page, NEXT_ITER, 5000L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, /* maxTotalRecords */ 5, THROTTLE_TIMEOUT_LARGE);
    List<Record> collected = new ArrayList<>();
    while (it.hasNext()) {
      collected.add(it.next());
    }

    assertEquals(5, collected.size());
    // After consuming exactly maxTotalRecords, fetchNextPage must not issue a second call.
    verify(client, times(1)).getRecords(isA(GetRecordsRequest.class));
  }

  @Test
  void testExpiredIteratorExceptionStopsReadGracefully() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenThrow(ExpiredIteratorException.builder().message("expired").build());

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);

    assertFalse(it.hasNext());
    assertFalse(it.getLastSequenceNumber().isPresent());
  }

  @Test
  void testEmptyPagesAboveLimitStopsFetching() {
    KinesisClient client = mock(KinesisClient.class);
    // Always return empty page with millisBehindLatest > 0 to simulate a shard that is not done
    // but consistently empty.  After MAX_EMPTY_RESPONSES_FROM_GET_RECORDS + 1 calls, stop.
    GetRecordsResponse emptyPage = response(Collections.emptyList(), NEXT_ITER, 5000L);
    when(client.getRecords(isA(GetRecordsRequest.class))).thenReturn(emptyPage);

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);
    assertFalse(it.hasNext());
    // MAX_EMPTY_RESPONSES_FROM_GET_RECORDS = 100, check is emptyPageCount++ > 100 (post-increment),
    // so the guard fires on the 102nd call (when emptyPageCount reaches 101).
    verify(client, times(102)).getRecords(isA(GetRecordsRequest.class));
  }

  // -------------------------------------------------------------------------
  // Checkpoint correctness
  // -------------------------------------------------------------------------

  /**
   * lastSequenceNumber must not advance until a page is fully consumed — it commits only when
   * hasNext() observes the current page is exhausted.
   */
  @Test
  void testLastSeqNotAdvancedUntilPageFullyConsumed() {
    KinesisClient client = mock(KinesisClient.class);
    List<Record> records = Arrays.asList(record("seq1"), record("seq2"), record("seq3"));
    when(client.getRecords(isA(GetRecordsRequest.class))).thenReturn(response(records, NEXT_ITER, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);

    // Before any consumption no checkpoint yet.
    assertFalse(it.getLastSequenceNumber().isPresent());

    it.next(); // seq1 — mid-page
    assertFalse(it.getLastSequenceNumber().isPresent());

    it.next(); // seq2
    it.next(); // seq3 — page iterator is exhausted but commit hasn't fired yet
    assertFalse(it.getLastSequenceNumber().isPresent());

    // hasNext() sees currentPage.hasNext() == false → commits pendingPageLastSeq.
    assertFalse(it.hasNext());
    assertEquals("seq3", it.getLastSequenceNumber().get());
  }

  /**
   * With two pages, lastSeq from page 1 must be committed before page 2 records are yielded.
   */
  @Test
  void testLastSeqAdvancesPageByPage() {
    KinesisClient client = mock(KinesisClient.class);
    List<Record> page1 = Arrays.asList(record("seq1"), record("seq2"));
    List<Record> page2 = Collections.singletonList(record("seq3"));
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenReturn(response(page1, NEXT_ITER, 5000L))
        .thenReturn(response(page2, null, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);

    it.next(); // seq1
    it.next(); // seq2 — exhausted page 1
    // Page 1 not committed yet (hasNext() for seq3 commits it).
    assertFalse(it.getLastSequenceNumber().isPresent());

    assertTrue(it.hasNext()); // commits page-1 lastSeq, fetches page 2
    assertEquals("seq2", it.getLastSequenceNumber().get());

    it.next(); // seq3
    assertFalse(it.hasNext()); // commits page-2 lastSeq
    assertEquals("seq3", it.getLastSequenceNumber().get());
  }

  // -------------------------------------------------------------------------
  // Throttle / adaptive rate-limit behaviour
  // -------------------------------------------------------------------------

  @Test
  void testFirstThrottleHalvesRequestLimit() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenThrow(throttleEx())
        .thenReturn(response(Collections.singletonList(record("seq1")), null, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);
    assertTrue(it.hasNext());

    ArgumentCaptor<GetRecordsRequest> captor = ArgumentCaptor.forClass(GetRecordsRequest.class);
    verify(client, times(2)).getRecords(captor.capture());
    List<GetRecordsRequest> reqs = captor.getAllValues();
    assertEquals(100, reqs.get(0).limit()); // original limit
    assertEquals(50,  reqs.get(1).limit()); // halved after throttle
  }

  @Test
  void testMultipleThrottlesHalveToFloorOfOne() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenThrow(throttleEx())   // 8 → 4
        .thenThrow(throttleEx())   // 4 → 2
        .thenThrow(throttleEx())   // 2 → 1
        .thenThrow(throttleEx())   // 1 → 1  (floor)
        .thenReturn(response(Collections.singletonList(record("seq1")), null, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 8, 1000, THROTTLE_TIMEOUT_LARGE);
    assertTrue(it.hasNext());

    ArgumentCaptor<GetRecordsRequest> captor = ArgumentCaptor.forClass(GetRecordsRequest.class);
    verify(client, times(5)).getRecords(captor.capture());
    List<GetRecordsRequest> reqs = captor.getAllValues();
    assertEquals(8, reqs.get(0).limit());
    assertEquals(4, reqs.get(1).limit());
    assertEquals(2, reqs.get(2).limit());
    assertEquals(1, reqs.get(3).limit());
    assertEquals(1, reqs.get(4).limit()); // stays at floor 1
  }

  @Test
  void testThrottleTimeoutExceededThrowsException() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class))).thenThrow(throttleEx());

    // throttleTimeoutMs = -1: any elapsed time exceeds it, so the first throttle immediately fails.
    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, -1L);

    assertThrows(HoodieReadFromSourceException.class, it::hasNext);
  }

  /**
   * Halve-and-hold: after a throttle + successful retry, subsequent page requests must continue
   * using the halved limit, not recover back to the original.
   */
  @Test
  void testHalveAndHoldLimitForSubsequentPages() {
    KinesisClient client = mock(KinesisClient.class);
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenThrow(throttleEx())                                                         // call 1: throttled → halve 100→50
        .thenReturn(response(Collections.singletonList(record("seq1")), NEXT_ITER, 5000L)) // call 2: success at 50
        .thenReturn(response(Collections.singletonList(record("seq2")), null,     0L));    // call 3: next page, still at 50

    KinesisSource.ShardRecordIterator it = iterator(client, 100, 1000, THROTTLE_TIMEOUT_LARGE);
    while (it.hasNext()) {
      it.next();
    }

    ArgumentCaptor<GetRecordsRequest> captor = ArgumentCaptor.forClass(GetRecordsRequest.class);
    verify(client, times(3)).getRecords(captor.capture());
    List<GetRecordsRequest> reqs = captor.getAllValues();
    assertEquals(100, reqs.get(0).limit()); // initial attempt before throttle
    assertEquals(50,  reqs.get(1).limit()); // halved, succeeded
    assertEquals(50,  reqs.get(2).limit()); // held — no recovery to 100
  }

  /**
   * A throttle on the first page (no records yet consumed) still halves the limit and retries,
   * rather than giving up or returning an empty result.
   */
  @Test
  void testFirstPageThrottleRetriesAndSucceeds() {
    KinesisClient client = mock(KinesisClient.class);
    List<Record> records = Arrays.asList(record("seq1"), record("seq2"));
    when(client.getRecords(isA(GetRecordsRequest.class)))
        .thenThrow(throttleEx())
        .thenReturn(response(records, null, 0L));

    KinesisSource.ShardRecordIterator it = iterator(client, 200, 1000, THROTTLE_TIMEOUT_LARGE);

    List<Record> collected = new ArrayList<>();
    while (it.hasNext()) {
      collected.add(it.next());
    }

    assertEquals(2, collected.size());
    assertEquals("seq2", it.getLastSequenceNumber().get());
    assertTrue(it.isReachedEndOfShard());

    // Verify the retry used the halved limit.
    ArgumentCaptor<GetRecordsRequest> captor = ArgumentCaptor.forClass(GetRecordsRequest.class);
    verify(client, times(2)).getRecords(captor.capture());
    assertEquals(200, captor.getAllValues().get(0).limit());
    assertEquals(100, captor.getAllValues().get(1).limit());
  }
}
